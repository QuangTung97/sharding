package sharding

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/concurrency"
	"github.com/QuangTung97/zk/curator"
)

// Sharding ...
type Sharding struct {
	parentPath string
	nodeID     string
	numShards  ShardID
	nodeAddr   string

	logger zk.Logger

	cur *curator.Curator

	obs *observerCore

	state *sessionState

	lockBegin func(sess *curator.Session)

	clientID curator.FakeClientID
}

type assignState struct {
	version int32
	shards  []ShardID
}

type sessionState struct {
	nodes            []string
	currentAssignMap map[string]assignState

	getAssignNodesCompleted  bool
	listActiveNodesCompleted bool
}

// NewNodeID creates a random node id with hex encoding and length = 16 bytes
func NewNodeID() string {
	var data [16]byte
	_, err := rand.Reader.Read(data[:])
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(data[:])
}

// New creates a Sharding object
func New(
	parentPath string, nodeID string,
	numShards ShardID, nodeAddr string,
	options ...Option,
) *Sharding {
	if len(nodeID) == 0 {
		panic("Invalid node id")
	}
	if len(nodeAddr) == 0 {
		panic("Invalid node address")
	}

	s := &Sharding{
		parentPath: parentPath,
		nodeID:     nodeID,
		numShards:  numShards,
		nodeAddr:   nodeAddr,

		logger: &defaultLoggerImpl{},
	}

	for _, fn := range options {
		fn(s)
	}

	controller := newContainerNodeController(parentPath, nodeID, nodeAddr)

	lock := concurrency.NewLock(s.getLockPath(), nodeID)

	startLeader := func(sess *curator.Session, next func(sess *curator.Session)) {
		lock.Start(sess, next)
		if s.obs != nil {
			s.obs.onStart(sess)
		}
	}

	s.cur = curator.NewChain(
		controller.onStart,
		startLeader,
		s.onLeaderCallback,
	)

	return s
}

func (s *Sharding) getLockPath() string {
	return s.parentPath + lockZNodeName
}

func (s *Sharding) getNodesPath() string {
	return s.parentPath + nodeZNodeName
}

func (s *Sharding) getAssignsPath() string {
	return s.parentPath + assignZNodeName
}

func (s *Sharding) onLeaderCallback(sess *curator.Session, _ func(sess *curator.Session)) {
	s.logger.Infof("Leader Started")

	s.state = &sessionState{
		currentAssignMap: map[string]assignState{},
	}
	s.listAssignNodes(sess)
	s.listActiveNodes(sess)
}

func (s *Sharding) getAssignNodeData(sess *curator.Session, nodeID string, counter *callbackCounter) {
	finish := counter.begin()
	sess.GetClient().Get(s.getAssignsPath()+"/"+nodeID, func(resp zk.GetResponse, err error) {
		defer finish()

		if err != nil {
			if errors.Is(err, zk.ErrConnectionClosed) {
				counter.addRetry(sess, s.listAssignNodes)
				return
			}
			if errors.Is(err, zk.ErrNoNode) {
				return
			}
			panic(err)
		}

		var assign assignData
		if err := json.Unmarshal(resp.Data, &assign); err != nil {
			panic(err)
		}
		s.putNodeAssignState(nodeID, resp.Stat.Version, assign.Shards)
	})
}

func (s *Sharding) putNodeAssignState(nodeID string, version int32, shards []ShardID) {
	old := s.state.currentAssignMap[nodeID]
	if old.version > version {
		panic("out of order responses")
	}

	s.state.currentAssignMap[nodeID] = assignState{
		version: version,
		shards:  shards,
	}
}

func (s *Sharding) startHandleNodeChanges(sess *curator.Session) {
	if s.state.listActiveNodesCompleted && s.state.getAssignNodesCompleted {
		s.handleNodesChanged(sess)
	}
}

type callbackCounter struct {
	count    int
	callback func()
}

func newCallbackCounter(callback func()) *callbackCounter {
	return &callbackCounter{
		callback: callback,
	}
}

func (c *callbackCounter) begin() func() {
	c.count++
	return func() {
		c.count--
		if c.count <= 0 {
			c.callback()
		}
	}
}

func (c *callbackCounter) addRetry(sess *curator.Session, fn func(sess *curator.Session)) {
	c.callback = func() {
		sess.AddRetry(fn)
	}
}

func (s *Sharding) listAssignNodes(sess *curator.Session) {
	sessMustChildren(sess, s.getAssignsPath(), func(resp zk.ChildrenResponse) {
		s.state.currentAssignMap = map[string]assignState{}

		counter := newCallbackCounter(func() {
			s.state.getAssignNodesCompleted = true
			s.startHandleNodeChanges(sess)
		})

		fn := counter.begin()
		for _, nodeID := range resp.Children {
			s.getAssignNodeData(sess, nodeID, counter)
		}
		fn()
	})
}

func (s *Sharding) listActiveNodes(sess *curator.Session) {
	sess.GetClient().ChildrenW(s.getNodesPath(), func(resp zk.ChildrenResponse, err error) {
		if err != nil {
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(s.listActiveNodes)
				return
			}
			panic(err)
		}

		s.state.nodes = resp.Children
		slices.Sort(s.state.nodes)

		s.state.listActiveNodesCompleted = true
		s.startHandleNodeChanges(sess)
	}, func(ev zk.Event) {
		if ev.Type == zk.EventNodeChildrenChanged {
			s.listActiveNodes(sess)
		}
	})
}

func (s *Sharding) getNodesSorted() []string {
	nodes := slices.Clone(s.state.nodes)
	slices.SortStableFunc(nodes, func(a, b string) int {
		assignA := s.state.currentAssignMap[a].shards
		assignB := s.state.currentAssignMap[b].shards
		return len(assignB) - len(assignA)
	})
	return nodes
}

func (s *Sharding) computeFreeShards() (map[ShardID]struct{}, []string) {
	nodeSet := map[string]struct{}{}
	for _, n := range s.state.nodes {
		nodeSet[n] = struct{}{}
	}

	freeShards := map[ShardID]struct{}{}
	for id := 0; id < int(s.numShards); id++ {
		freeShards[ShardID(id)] = struct{}{}
	}

	var deleted []string
	for nodeID := range s.state.currentAssignMap {
		_, ok := nodeSet[nodeID]
		if !ok {
			deleted = append(deleted, nodeID)
			continue
		}
	}

	return freeShards, deleted
}

func (s *Sharding) handleNodesChanged(sess *curator.Session) {
	n := ShardID(len(s.state.nodes))
	minShardsPerNode := s.numShards / n
	maxShardsPerNode := (s.numShards + n - 1) / n
	numMax := s.numShards - minShardsPerNode*n

	nodes := s.getNodesSorted()

	_, deletedNodes := s.computeFreeShards()
	allocated := map[ShardID]struct{}{}

	counter := newCallbackCounter(func() {
		s.handleNodesChanged(sess)
	})

	for i := 0; i < int(numMax); i++ {
		s.updateIfChanged(sess, nodes[i], int(maxShardsPerNode), allocated, counter)
	}

	for i := int(numMax); i < int(n); i++ {
		s.updateIfChanged(sess, nodes[i], int(minShardsPerNode), allocated, counter)
	}

	slices.Sort(deletedNodes)
	for _, nodeID := range deletedNodes {
		s.deleteAssignNode(sess, nodeID, counter)
	}
}

func computeFreeShards(allocated map[ShardID]struct{}, numShards ShardID) []ShardID {
	var list []ShardID
	for id := ShardID(0); id < numShards; id++ {
		_, ok := allocated[id]
		if ok {
			continue
		}
		list = append(list, id)
	}
	return list
}

func (s *Sharding) updateIfChanged(
	sess *curator.Session, nodeID string, expectLen int,
	allocatedShards map[ShardID]struct{},
	counter *callbackCounter,
) {
	oldShards := s.state.currentAssignMap[nodeID].shards
	current := make([]ShardID, 0, len(oldShards))
	for _, id := range oldShards {
		_, existed := allocatedShards[id]
		if existed {
			continue
		}
		current = append(current, id)
	}
	slices.Sort(current)

	addToAllocated := func(curr []ShardID) {
		for _, id := range curr {
			allocatedShards[id] = struct{}{}
		}
	}

	if len(current) > expectLen {
		current = slices.Clone(current[:expectLen])
		addToAllocated(current)
		s.upsertAssigns(sess, nodeID, current, counter)
		return
	}
	if len(current) < expectLen {
		addToAllocated(current)

		list := computeFreeShards(allocatedShards, s.numShards)
		missing := expectLen - len(current)

		addToAllocated(list[:missing])

		current = slices.Clone(append(current, list[:missing]...))
		s.upsertAssigns(sess, nodeID, current, counter)
		return
	}

	// No Change
	addToAllocated(current)
	oldShards = slices.Clone(oldShards)
	slices.Sort(oldShards)
	if !slices.Equal(oldShards, current) {
		s.upsertAssigns(sess, nodeID, current, counter)
	}
}

func marshalAssignNodeData(shards []ShardID) []byte {
	data, err := json.Marshal(assignData{
		Shards: shards,
	})
	if err != nil {
		panic(err)
	}
	return data
}

func (s *Sharding) getNodeAssignPath(nodeID string) string {
	return s.getAssignsPath() + "/" + nodeID
}

func (s *Sharding) upsertAssigns(
	sess *curator.Session, nodeID string, shards []ShardID,
	counter *callbackCounter,
) {
	prev, ok := s.state.currentAssignMap[nodeID]
	if ok {
		s.updateAssignNode(sess, nodeID, shards, prev, counter)
	} else {
		s.createAssignNode(sess, nodeID, shards, counter)
	}
}

func (s *Sharding) retryListAssignsIfErr(
	sess *curator.Session, err error, counter *callbackCounter,
) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, zk.ErrConnectionClosed) {
		counter.addRetry(sess, s.listAssignNodes)
		return true
	}

	if isOneOfErrors(err,
		zk.ErrBadVersion, zk.ErrNodeExists, zk.ErrNoNode,
	) {
		return true
	}

	panic(err)
}

func isOneOfErrors(err error, errorList ...error) bool {
	for _, cmpErr := range errorList {
		if errors.Is(err, cmpErr) {
			return true
		}
	}
	return false
}

func (s *Sharding) updateAssignNode(
	sess *curator.Session, nodeID string,
	shards []ShardID, prev assignState,
	counter *callbackCounter,
) {
	pathVal := s.getNodeAssignPath(nodeID)
	data := marshalAssignNodeData(shards)

	finish := counter.begin()
	sess.GetClient().Set(pathVal, data, prev.version, func(resp zk.SetResponse, err error) {
		defer finish()

		if s.retryListAssignsIfErr(sess, err, counter) {
			return
		}
		s.putNodeAssignState(nodeID, resp.Stat.Version, shards)
	})
}

func (s *Sharding) createAssignNode(
	sess *curator.Session, nodeID string,
	shards []ShardID, counter *callbackCounter,
) {
	pathVal := s.getNodeAssignPath(nodeID)
	data := marshalAssignNodeData(shards)

	finish := counter.begin()
	sess.GetClient().Create(pathVal, data, 0, func(resp zk.CreateResponse, err error) {
		defer finish()
		if s.retryListAssignsIfErr(sess, err, counter) {
			return
		}
		s.putNodeAssignState(nodeID, 0, shards)
	})
}

func (s *Sharding) deleteAssignNode(sess *curator.Session, nodeID string, counter *callbackCounter) {
	version := s.state.currentAssignMap[nodeID].version

	finish := counter.begin()
	sess.GetClient().Delete(s.getAssignsPath()+"/"+nodeID, version, func(resp zk.DeleteResponse, err error) {
		defer finish()

		if s.retryListAssignsIfErr(sess, err, counter) {
			return
		}
		delete(s.state.currentAssignMap, nodeID)
	})
}

// GetCurator is used for input of the curator.Client.Start() method
func (s *Sharding) GetCurator() *curator.Curator {
	return s.cur
}

// ========================================
// Logic for Creating Container Nodes
// ========================================
type containerNodeController struct {
	state      *nodeControllerState
	parentPath string
	next       func(sess *curator.Session)

	nodeID   string
	nodeAddr string
}

type nodeControllerState struct {
	lockCreated    bool
	nodesCreated   bool
	assignsCreated bool
}

func newContainerNodeController(
	parent string,
	nodeID string, nodeAddr string,
) *containerNodeController {
	return &containerNodeController{
		parentPath: parent,
		nodeID:     nodeID,
		nodeAddr:   nodeAddr,
	}
}

func (c *containerNodeController) onStart(sess *curator.Session, next func(sess *curator.Session)) {
	c.next = next
	c.state = &nodeControllerState{}
	c.createInitNodes(sess)
}

func (c *containerNodeController) createInitNodes(sess *curator.Session) {
	sessMustCreatePersistence(sess, c.getLockPath(), func(resp zk.CreateResponse) {
		c.state.lockCreated = true
		c.createCompleted(sess)
	})

	sessMustCreatePersistence(sess, c.getNodesPath(), func(resp zk.CreateResponse) {
		if len(c.nodeID) > 0 {
			c.createEphemeralNode(sess)
		} else {
			c.state.nodesCreated = true
			c.createCompleted(sess)
		}
	})

	sessMustCreatePersistence(sess, c.getAssignsPath(), func(resp zk.CreateResponse) {
		c.state.assignsCreated = true
		c.createCompleted(sess)
	})
}

func (c *containerNodeController) createEphemeralNode(sess *curator.Session) {
	pathVal := c.getNodesPath() + "/" + c.nodeID
	data := nodeData{Address: c.nodeAddr}.marshalJSON()

	sessMustCreateWithData(sess, pathVal, zk.FlagEphemeral, data, func(resp zk.CreateResponse) {
		c.state.nodesCreated = true
		c.createCompleted(sess)
	})
}

func (c *containerNodeController) createCompleted(sess *curator.Session) {
	if c.state.lockCreated && c.state.nodesCreated && c.state.assignsCreated {
		c.next(sess)
	}
}

func (c *containerNodeController) getLockPath() string {
	return c.parentPath + lockZNodeName
}

func (c *containerNodeController) getNodesPath() string {
	return c.parentPath + nodeZNodeName
}

func (c *containerNodeController) getAssignsPath() string {
	return c.parentPath + assignZNodeName
}
