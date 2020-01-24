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
	"encoding/binary"
	"github.com/atomix/go-framework/pkg/atomix/cluster"
	"github.com/atomix/go-framework/pkg/atomix/node"
	"github.com/atomix/go-framework/pkg/atomix/service"
	streams "github.com/atomix/go-framework/pkg/atomix/stream"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/raft/raftpb"
	"io"
	"sync"
	"time"
)

// newStateMachine returns a new primitive state machine
func newStateMachine(cluster cluster.Cluster, registry *node.Registry, streams *streamManager) *StateMachine {
	fsm := &StateMachine{
		node:    cluster.MemberID,
		streams: streams,
	}
	fsm.state = node.NewPrimitiveStateMachine(registry, fsm)
	return fsm
}

type StateMachine struct {
	node      string
	state     node.StateMachine
	streams   *streamManager
	index     uint64
	timestamp time.Time
	operation service.OperationType
	mu        sync.RWMutex
}

func (s *StateMachine) Node() string {
	return s.node
}

func (s *StateMachine) Index() uint64 {
	return s.index
}

func (s *StateMachine) Timestamp() time.Time {
	return s.timestamp
}

func (s *StateMachine) OperationType() service.OperationType {
	return s.operation
}

func (s *StateMachine) Apply(entry *raftpb.Entry) error {
	tsEntry := &Entry{}
	if err := proto.Unmarshal(entry.Data, tsEntry); err != nil {
		return err
	}

	stream := s.streams.getStream(tsEntry.StreamID)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.index = entry.Index
	if tsEntry.Timestamp.After(s.timestamp) {
		s.timestamp = tsEntry.Timestamp
	}
	s.operation = service.OpTypeCommand
	s.state.Command(tsEntry.Value, stream)
	return nil
}

func (s *StateMachine) Query(value []byte, stream streams.WriteStream) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.operation = service.OpTypeQuery
	s.state.Query(value, stream)
	return nil
}

func (s *StateMachine) Snapshot(writer io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(s.timestamp.Second()))
	if _, err := writer.Write(bytes); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(bytes, uint64(s.timestamp.Nanosecond()))
	if _, err := writer.Write(bytes); err != nil {
		return err
	}
	return s.state.Snapshot(writer)
}

func (s *StateMachine) Restore(reader io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	bytes := make([]byte, 8)
	if _, err := reader.Read(bytes); err != nil {
		return err
	}
	secs := int64(binary.BigEndian.Uint64(bytes))
	if _, err := reader.Read(bytes); err != nil {
		return err
	}
	nanos := int64(binary.BigEndian.Uint64(bytes))
	s.timestamp = time.Unix(secs, nanos)
	return s.state.Install(reader)
}
