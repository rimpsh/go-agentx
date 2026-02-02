// Copyright 2018 The agentx authors
// Licensed under the LGPLv3 with static-linking exception.
// See LICENCE file for details.

package pdu

import (
	"bytes"
)

// Notify defines the pdu notify packet.
type Notify struct {
	Variables Variables
}

// Type returns the pdu packet type.
func (n *Notify) Type() Type {
	return TypeNotify
}

// MarshalBinary returns the pdu packet as a slice of bytes.
func (n *Notify) MarshalBinary() ([]byte, error) {
	buffer := &bytes.Buffer{}

	vBytes, err := n.Variables.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buffer.Write(vBytes)

	return buffer.Bytes(), nil
}

// UnmarshalBinary sets the packet structure from the provided slice of bytes.
func (n *Notify) UnmarshalBinary(data []byte) error {
	return nil
}
