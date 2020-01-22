// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"context"
	streams "github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"github.com/gogo/protobuf/proto"
	"time"
)

const clientTimeout = 15 * time.Second

// newClient returns a new Raft consensus protocol client
func newClient(raft *Raft, fsm *StateMachine, members map[int]string, streams *streamManager) *Client {
	return &Client{
		raft:    raft,
		state:   fsm,
		members: members,
		streams: streams,
	}
}

// Client is the Raft client
type Client struct {
	raft    *Raft
	state   *StateMachine
	members map[int]string
	streams *streamManager
}

func (c *Client) MustLeader() bool {
	return true
}

func (c *Client) IsLeader() bool {
	return c.raft.IsLeader()
}

func (c *Client) Leader() string {
	leader := c.raft.Leader()
	if leader == 0 {
		return ""
	}
	return c.members[leader]
}

func (c *Client) Write(ctx context.Context, input []byte, stream streams.WriteStream) error {
	streamID, stream := c.streams.addStream(stream)
	entry := &Entry{
		Value:     input,
		StreamID:  streamID,
		Timestamp: time.Now(),
	}
	bytes, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()
	if err := c.raft.Propose(ctx, bytes); err != nil {
		stream.Close()
		return err
	}
	return nil
}

func (c *Client) Read(ctx context.Context, input []byte, stream streams.WriteStream) error {
	return c.state.Query(input, stream)
}
