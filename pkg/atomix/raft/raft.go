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
	"bytes"
	"context"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.uber.org/zap/buffer"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// A key-value stream backed by raft
type Raft struct {
	fsm      *StateMachine
	commitCh chan *raftpb.Entry // entries committed to log (k,v)
	errorCh  chan error         // errors from raft session

	id        int      // client ID for raft session
	peers     []string // raft peer URLs
	waldir    string   // path to WAL directory
	snapdir   string   // path to snapshot directory
	lastIndex uint64   // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter *snap.Snapshotter

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeCh and read errorCh.
func newRaftNode(id int, peers []string, fsm *StateMachine) *Raft {
	return &Raft{
		fsm:       fsm,
		commitCh:  make(chan *raftpb.Entry, 1000),
		errorCh:   make(chan error, 100),
		id:        id,
		peers:     peers,
		waldir:    "/var/lib/atomix/wal",
		snapdir:   "/var/lib/atomix/snap",
		snapCount: defaultSnapshotCount,
		stopc:     make(chan struct{}),
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),
	}
}

func (r *Raft) Leader() int {
	if r.transport.LeaderStats == nil || r.transport.LeaderStats.Leader == "" {
		return 0
	}
	id, _ := strconv.Atoi(r.transport.LeaderStats.Leader)
	return id
}

func (r *Raft) IsLeader() bool {
	return r.Leader() == r.id
}

func (r *Raft) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := r.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := r.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return r.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (r *Raft) entriesToApply(entries []raftpb.Entry) (nents []raftpb.Entry) {
	if len(entries) == 0 {
		return entries
	}
	firstIdx := entries[0].Index
	if firstIdx > r.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, r.appliedIndex)
	}
	if r.appliedIndex-firstIdx+1 < uint64(len(entries)) {
		nents = entries[r.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (r *Raft) publishEntries(entries []raftpb.Entry) bool {
	for i := range entries {
		entry := entries[i]
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entries[i].Data) == 0 {
				// ignore empty messages
				break
			}
			select {
			case r.commitCh <- &entry:
			case <-r.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(entries[i].Data)
			r.confState = *r.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					r.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(r.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				r.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		r.appliedIndex = entry.Index

		// special nil commit to signal replay has finished
		if entry.Index == r.lastIndex {
			select {
			case r.commitCh <- nil:
			case <-r.stopc:
				return false
			}
		}
	}
	return true
}

func (r *Raft) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := r.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (r *Raft) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(r.waldir) {
		if err := os.Mkdir(r.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), r.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), r.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (r *Raft) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", r.id)
	snapshot := r.loadSnapshot()
	w := r.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	r.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		r.raftStorage.ApplySnapshot(*snapshot)
	}
	r.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	r.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		r.lastIndex = ents[len(ents)-1].Index
	} else {
		r.commitCh <- nil
	}
	return w
}

func (r *Raft) writeError(err error) {
	r.stopHTTP()
	close(r.commitCh)
	r.errorCh <- err
	close(r.errorCh)
	r.node.Stop()
}

func (r *Raft) startRaft() {
	if !fileutil.Exist(r.snapdir) {
		if err := os.Mkdir(r.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	r.snapshotter = snap.New(zap.NewExample(), r.snapdir)

	go func() {
		r.applyCommits()
		go r.applyCommits()
	}()

	oldwal := wal.Exist(r.waldir)
	r.wal = r.replayWAL()

	rpeers := make([]raft.Peer, len(r.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(r.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   r.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldwal {
		r.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		r.node = raft.StartNode(c, startPeers)
	}

	r.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(r.id),
		ClusterID:   0x1000,
		Raft:        r,
		ServerStats: v2stats.NewServerStats("", ""),
		LeaderStats: v2stats.NewLeaderStats(strconv.Itoa(r.id)),
		ErrorC:      make(chan error),
	}

	r.transport.Start()
	for i := range r.peers {
		if i+1 != r.id {
			r.transport.AddPeer(types.ID(i+1), []string{r.peers[i]})
		}
	}

	go r.serveRaft()
	go r.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (r *Raft) stop() {
	r.stopHTTP()
	close(r.commitCh)
	close(r.errorCh)
	r.node.Stop()
}

func (r *Raft) stopHTTP() {
	r.transport.Stop()
	close(r.httpstopc)
	<-r.httpdonec
}

func (r *Raft) Propose(ctx context.Context, data []byte) error {
	return r.node.Propose(ctx, data)
}

func (r *Raft) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", r.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", r.snapshotIndex)

	if snapshotToSave.Metadata.Index <= r.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, r.appliedIndex)
	}
	r.commitCh <- nil // trigger load snapshot

	r.confState = snapshotToSave.Metadata.ConfState
	r.snapshotIndex = snapshotToSave.Metadata.Index
	r.appliedIndex = snapshotToSave.Metadata.Index
}

const snapshotCatchUpEntriesN uint64 = 10000

func (r *Raft) maybeTriggerSnapshot() {
	if r.appliedIndex-r.snapshotIndex <= r.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", r.appliedIndex, r.snapshotIndex)
	buf := &buffer.Buffer{}
	err := r.fsm.Snapshot(buf)
	if err != nil {
		log.Panic(err)
	}
	data := buf.Bytes()
	snap, err := r.raftStorage.CreateSnapshot(r.appliedIndex, &r.confState, data)
	if err != nil {
		panic(err)
	}
	if err := r.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if r.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = r.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := r.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	r.snapshotIndex = r.appliedIndex
}

func (r *Raft) recoverFromSnapshot(snapshot []byte) error {
	buf := bytes.NewBuffer(snapshot)
	return r.fsm.Restore(buf)
}

func (r *Raft) serveChannels() {
	snap, err := r.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	r.confState = snap.Metadata.ConfState
	r.snapshotIndex = snap.Metadata.Index
	r.appliedIndex = snap.Metadata.Index

	defer r.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			r.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-r.node.Ready():
			r.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				r.saveSnap(rd.Snapshot)
				r.raftStorage.ApplySnapshot(rd.Snapshot)
				r.publishSnapshot(rd.Snapshot)
			}
			r.raftStorage.Append(rd.Entries)
			r.transport.Send(rd.Messages)
			if ok := r.publishEntries(r.entriesToApply(rd.CommittedEntries)); !ok {
				r.stop()
				return
			}
			r.maybeTriggerSnapshot()
			r.node.Advance()

		case err := <-r.transport.ErrorC:
			r.writeError(err)
			return

		case <-r.stopc:
			r.stop()
			return
		}
	}
}

func (r *Raft) applyCommits() {
	for entry := range r.commitCh {
		if entry == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := r.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := r.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		if err := r.fsm.Apply(entry); err != nil {
			log.Fatalf("an error occurred when applying an entry (%v)", err)
		}
	}
	if err, ok := <-r.errorCh; ok {
		log.Fatal(err)
	}
}

func (r *Raft) serveRaft() {
	url, err := url.Parse(r.peers[r.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, r.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: r.transport.Handler()}).Serve(ln)
	select {
	case <-r.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(r.httpdonec)
}

func (r *Raft) Process(ctx context.Context, m raftpb.Message) error {
	return r.node.Step(ctx, m)
}
func (r *Raft) IsIDRemoved(id uint64) bool                           { return false }
func (r *Raft) ReportUnreachable(id uint64)                          {}
func (r *Raft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
