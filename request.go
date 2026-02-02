// Copyright 2018 The agentx authors
// Licensed under the LGPLv3 with static-linking exception.
// See LICENCE file for details.

package agentx

import (
	"context"

	"github.com/rimpsh/go-agentx/pdu"
)

type request struct {
	headerPacket *pdu.HeaderPacket
	responseChan chan *pdu.HeaderPacket
	ctx          context.Context
}

func (r *request) String() string {
	return "(request " + r.headerPacket.String() + ")"
}
