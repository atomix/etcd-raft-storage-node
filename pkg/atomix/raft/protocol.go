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
	"fmt"
	"github.com/atomix/etcd-raft-replica/pkg/atomix/raft/config"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"sort"
)

// NewProtocol returns a new Raft Protocol instance
func NewProtocol(config *config.ProtocolConfig) *Protocol {
	return &Protocol{
		config: config,
	}
}

// Protocol is an implementation of the Client interface providing the Raft consensus protocol
type Protocol struct {
	node.Protocol
	config *config.ProtocolConfig
	client *Client
	server *Server
}

// Start starts the Raft protocol
func (p *Protocol) Start(config cluster.Cluster, registry *node.Registry) error {
	streams := newStreamManager()
	fsm := newStateMachine(config, registry, streams)

	nodes := make([]cluster.Member, 0, len(config.Members))
	for _, member := range config.Members {
		nodes = append(nodes, member)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	var id int
	members := make(map[int]string)
	peers := make([]string, len(nodes))
	for i, member := range nodes {
		members[i+1] = fmt.Sprintf("%s:%d", member.Host, member.APIPort)
		peers[i] = fmt.Sprintf("http://%s:%d", member.Host, member.ProtocolPort)
		if member.ID == config.MemberID {
			id = i + 1
		}
	}

	raft := newRaftNode(id, peers, fsm)
	p.server = newServer(config, raft, fsm)
	p.client = newClient(raft, fsm, members, streams)
	return p.server.Start()
}

// Client returns the Raft protocol client
func (p *Protocol) Client() node.Client {
	return p.client
}

// Stop stops the Raft protocol
func (p *Protocol) Stop() error {
	return p.server.Stop()
}
