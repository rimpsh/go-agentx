// Copyright 2018 The agentx authors
// Licensed under the LGPLv3 with static-linking exception.
// See LICENCE file for details.

package agentx

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/rimpsh/go-agentx/pdu"
	"github.com/rimpsh/go-agentx/value"
)

// Client defines an agentx client.
type Client struct {
	logger      *slog.Logger
	network     string
	address     string
	options     dialOptions
	connMu      sync.RWMutex
	conn        net.Conn
	requestChan chan *request
	sessionsMu  sync.RWMutex
	sessions    map[uint32]*Session

	// This is used to signal receiver, transmitter and dispatcher goroutines to
	// stop.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Dial connects to the provided agentX endpoint.
func Dial(network, address string, opts ...DialOption) (*Client, error) {
	options := dialOptions{}
	for _, dialOption := range opts {
		dialOption(&options)
	}

	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, fmt.Errorf("dial %s %s: %w", network, address, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		logger:      options.logger,
		network:     network,
		address:     address,
		options:     options,
		conn:        conn,
		requestChan: make(chan *request),
		sessions:    make(map[uint32]*Session),
		ctx:         ctx,
		cancel:      cancel,
	}

	if c.logger == nil {
		c.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	tx := c.runTransmitter()
	rx := c.runReceiver()
	c.runDispatcher(tx, rx)

	return c, nil
}

// Close tears down the client.
func (c *Client) Close() error {
	// Signal client goroutines to stop
	c.cancel()

	done := make(chan struct{})
	go func() {
		// Wait for goroutines to stop
		c.wg.Wait()
		close(done)
	}()

	// Close the connection (this will cause receiver to exit)
	if err := c.conn.Close(); err != nil {
		c.logger.Debug("error closing connection", slog.Any("err", err))
	}

	select {
	case <-done:
		c.logger.Debug("all goroutines stopped gracefully")
		return nil
	case <-time.After(5 * time.Second):
		c.logger.Warn("timeout waiting for goroutines to stop")
		return fmt.Errorf("timeout waiting for goroutines to stop")
	}
}

// Session sets up a new session.
func (c *Client) Session(nameOID value.OID, name string, handler Handler) (*Session, error) {
	s, err := openSession(c, nameOID, name, handler)
	if err != nil {
		return nil, fmt.Errorf("open session: %w", err)
	}

	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()

	c.sessions[s.ID()] = s
	return s, nil
}

func (c *Client) runTransmitter() chan *pdu.HeaderPacket {
	tx := make(chan *pdu.HeaderPacket)

	c.wg.Go(func() {
		defer c.logger.Debug("transmitter goroutine stopped")

		for {
			select {
			case <-c.ctx.Done():
				return
			case headerPacket, ok := <-tx:
				if !ok {
					return
				}

				headerPacketBytes, err := headerPacket.MarshalBinary()
				if err != nil {
					c.logger.Debug("header packet marshal error", slog.Any("err", err))
					continue
				}
				c.connMu.RLock()
				currentConn := c.conn
				c.connMu.RUnlock()

				writer := bufio.NewWriter(currentConn)
				if _, err := writer.Write(headerPacketBytes); err != nil {
					c.logger.Debug("header packet write error", slog.Any("err", err))
					continue
				}
				if err := writer.Flush(); err != nil {
					c.logger.Debug("header packet flush error", slog.Any("err", err))
					continue
				}
			}
		}
	})

	return tx
}

func (c *Client) runReceiver() chan *pdu.HeaderPacket {
	rx := make(chan *pdu.HeaderPacket)

	c.wg.Go(func() {
		defer close(rx)
		defer c.logger.Debug("receiver goroutine stopped")

	mainLoop:
		for {
			c.connMu.RLock()
			currentConn := c.conn
			c.connMu.RUnlock()

			reader := bufio.NewReader(currentConn)
			headerBytes := make([]byte, pdu.HeaderSize)
			if _, err := reader.Read(headerBytes); err != nil {
				if opErr, ok := err.(*net.OpError); ok && strings.HasSuffix(opErr.Error(), "use of closed network connection") {
					return
				}
				if err == io.EOF {
					c.logger.Info("lost connection", slog.Duration("re-connect-in", c.options.reconnectInterval))
				reopenLoop:
					for {
						select {
						case <-c.ctx.Done():
							return
						case <-time.After(c.options.reconnectInterval):
						}

						conn, err := net.Dial(c.network, c.address)
						if err != nil {
							c.logger.Error("re-connect error", slog.Any("err", err))
							continue reopenLoop
						}
						c.connMu.Lock()
						c.conn = conn
						c.connMu.Unlock()
						go func() {
							c.sessionsMu.Lock()
							sessionsReopen := make([]*Session, 0, len(c.sessions))
							for _, session := range c.sessions {
								sessionsReopen = append(sessionsReopen, session)
							}
							// clear sessions map
							c.sessions = make(map[uint32]*Session)
							c.sessionsMu.Unlock()

							// re-open sessions
							for _, session := range sessionsReopen {
								if err := session.reopen(); err != nil {
									c.logger.Error("re-open error", slog.Any("err", err))
									continue
								}

								c.sessionsMu.Lock()
								c.sessions[session.ID()] = session
								c.sessionsMu.Unlock()
							}
							c.logger.Info("re-connect successful")
						}()
						continue mainLoop
					}
				}
				c.logger.Error("unexpected error reading header", slog.Any("err", err))
				return
			}

			header := &pdu.Header{}
			if err := header.UnmarshalBinary(headerBytes); err != nil {
				c.logger.Error("error unmarshaling header", slog.Any("err", err))
				return
			}

			var packet pdu.Packet
			switch header.Type {
			case pdu.TypeResponse:
				packet = &pdu.Response{}
			case pdu.TypeGet:
				packet = &pdu.Get{}
			case pdu.TypeGetNext:
				packet = &pdu.GetNext{}
			default:
				c.logger.Error("unable to handle packet", slog.String("packet-type", header.Type.String()))
				continue
			}

			packetBytes := make([]byte, header.PayloadLength)
			if _, err := reader.Read(packetBytes); err != nil {
				c.logger.Error("error reading packet bytes", slog.Any("err", err))
				return
			}

			if err := packet.UnmarshalBinary(packetBytes); err != nil {
				c.logger.Error("error unmarshaling packet", slog.Any("err", err))
				return
			}

			select {
			case <-c.ctx.Done():
				return
			case rx <- &pdu.HeaderPacket{Header: header, Packet: packet}:
			}
		}
	})

	return rx
}

func (c *Client) runDispatcher(tx, rx chan *pdu.HeaderPacket) {
	c.wg.Go(func() {
		defer close(tx)
		defer c.logger.Debug("dispatcher goroutine stopped")

		currentPacketID := uint32(0)
		responseChansMu := sync.Mutex{}
		responseChans := make(map[uint32]chan *pdu.HeaderPacket)

		for {
			select {
			case <-c.ctx.Done():
				// Cleanup: close all pending response channels
				responseChansMu.Lock()
				for _, ch := range responseChans {
					close(ch)
				}
				responseChansMu.Unlock()
				return

			case request := <-c.requestChan:
				// log.Printf(">: %v", request)
				request.headerPacket.Header.PacketID = currentPacketID

				responseChansMu.Lock()
				responseChans[currentPacketID] = request.responseChan
				responseChansMu.Unlock()

				packetID := currentPacketID
				go func() {
					<-request.ctx.Done()
					responseChansMu.Lock()
					defer responseChansMu.Unlock()
					delete(responseChans, packetID)
				}()

				currentPacketID++

				select {
				case <-c.ctx.Done():
					return
				case tx <- request.headerPacket:
				}

			case headerPacket, ok := <-rx:
				if !ok {
					return
				}

				// log.Printf("<: %v", headerPacket)
				packetID := headerPacket.Header.PacketID

				responseChansMu.Lock()
				responseChan, ok := responseChans[packetID]
				delete(responseChans, packetID)
				responseChansMu.Unlock()

				if ok {
					select {
					case responseChan <- headerPacket:
					case <-c.ctx.Done():
						return
					}
				} else {
					c.sessionsMu.RLock()
					session, ok := c.sessions[headerPacket.Header.SessionID]
					c.sessionsMu.RUnlock()
					if ok {
						select {
						case <-c.ctx.Done():
							return
						case tx <- session.handle(headerPacket):
						}
					} else {
						c.logger.Error("got packet without session", slog.String("packet-type", headerPacket.Header.Type.String()))
					}
				}
			}
		}
	})
}

func (c *Client) request(hp *pdu.HeaderPacket) (*pdu.HeaderPacket, error) {
	if c.ctx.Err() != nil {
		return nil, fmt.Errorf("client is closed")
	}

	ctx, cancel := context.WithTimeout(c.ctx, c.options.timeout)
	defer cancel()

	responseChan := make(chan *pdu.HeaderPacket, 1)

	request := &request{
		headerPacket: hp,
		responseChan: responseChan,
		ctx:          ctx,
	}

	select {
	case c.requestChan <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case headerPacket, ok := <-responseChan:
		if !ok {
			return nil, fmt.Errorf("response channel closed")
		}
		return headerPacket, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
