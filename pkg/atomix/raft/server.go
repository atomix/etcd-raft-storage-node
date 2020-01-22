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
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
)

// newServer returns a new protocol server
func newServer(cluster cluster.Cluster, raft *Raft, fsm *StateMachine) *Server {
	return &Server{
		cluster: cluster,
		raft:    raft,
		fsm:     fsm,
	}
}

// Server is a Raft server
type Server struct {
	cluster cluster.Cluster
	raft    *Raft
	fsm     *StateMachine
}

// Start starts the server
func (s *Server) Start() error {
	s.raft.startRaft()
	return nil
}

// Stop stops the server
func (s *Server) Stop() error {
	s.raft.stop()
	return nil
}
