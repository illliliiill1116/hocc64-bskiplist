package linearcheck

import (
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

type KVPair struct {
	Key uint64
	Val uint64
}

const (
	opInsert = iota
	opDelete
	opGet
	opScan
)

type mapOp struct {
	kind  int
	key   uint64
	val   uint64
	start uint64
	count int
}

type mapResult struct {
	retCode int
	outVal  uint64
	pairs   []KVPair
}

type bslState struct {
	m map[uint64]uint64
}

func newBslState() interface{} {
	return bslState{m: make(map[uint64]uint64)}
}

func copyBslState(s bslState) bslState {
	m := make(map[uint64]uint64, len(s.m))
	for k, v := range s.m {
		m[k] = v
	}
	return bslState{m: m}
}

func bslStep(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
	s := copyBslState(state.(bslState))
	op := input.(mapOp)
	res := output.(mapResult)

	switch op.kind {
	case opInsert:
		s.m[op.key] = op.val
		return res.retCode == 1, s

	case opDelete:
		_, existed := s.m[op.key]
		delete(s.m, op.key)
		if existed {
			return res.retCode == 1, s
		}
		return res.retCode == 0, s

	case opGet:
		v, found := s.m[op.key]
		if found {
			return res.retCode == 1 && res.outVal == v, s
		}
		return res.retCode == 0, s

	case opScan:
		var expected []KVPair
		for k, v := range s.m {
			if k >= op.start {
				expected = append(expected, KVPair{k, v})
			}
		}
		sort.Slice(expected, func(i, j int) bool {
			return expected[i].Key < expected[j].Key
		})
		if len(expected) > op.count {
			expected = expected[:op.count]
		}

		got := res.pairs
		if len(got) != len(expected) {
			return false, s
		}
		for i := range expected {
			if got[i] != expected[i] {
				return false, s
			}
		}
		return true, s
	}
	return false, s
}

func bslEqual(s1, s2 interface{}) bool {
	m1 := s1.(bslState).m
	m2 := s2.(bslState).m
	if len(m1) != len(m2) {
		return false
	}
	for k, v := range m1 {
		if v2, ok := m2[k]; !ok || v != v2 {
			return false
		}
	}
	return true
}

var bslModel = porcupine.Model{
	Init:  newBslState,
	Step:  bslStep,
	Equal: bslEqual,
}

func runStressTest(
	m *BSLMap,
	numThreads int,
	opsPerThread int,
	keyRange uint64,
	scanLength int,
	weights map[int]int,
) []porcupine.Operation {

	var (
		mu            sync.Mutex
		records       []porcupine.Operation
		wg            sync.WaitGroup
		clientCounter int64
	)

	type weightRange struct {
		op  int
		max int
	}
	var ranges []weightRange
	current := 0
	for op, w := range weights {
		current += w
		ranges = append(ranges, weightRange{op, current})
	}
	totalWeight := current

	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clientId := int(atomic.AddInt64(&clientCounter, 1))
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientId)))
			local := make([]porcupine.Operation, 0, opsPerThread)

			for i := 0; i < opsPerThread; i++ {
				r := rng.Intn(totalWeight)
				var opKind int
				for _, wr := range ranges {
					if r < wr.max {
						opKind = wr.op
						break
					}
				}

				key := rng.Uint64()%keyRange + 1
				val := rng.Uint64()%1000 + 1
				var input mapOp
				var result mapResult

				callT := time.Now().UnixNano()
				switch opKind {
				case opInsert:
					input = mapOp{kind: opInsert, key: key, val: val}
					result = mapResult{retCode: m.Insert(key, val)}
				case opDelete:
					input = mapOp{kind: opDelete, key: key}
					result = mapResult{retCode: m.Delete(key)}
				case opGet:
					input = mapOp{kind: opGet, key: key}
					code, outVal := m.Get(key)
					result = mapResult{retCode: code, outVal: outVal}
				case opScan:
					startKey := rng.Uint64()%keyRange + 1
					input = mapOp{kind: opScan, start: startKey, count: scanLength}
					result = mapResult{pairs: m.Scan(startKey, scanLength)}
				}
				retT := time.Now().UnixNano()

				local = append(local, porcupine.Operation{
					ClientId: clientId,
					Input:    input,
					Output:   result,
					Call:     callT,
					Return:   retT,
				})
			}
			mu.Lock()
			records = append(records, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return records
}

func TestSequentialSanity(t *testing.T) {
	m := NewBSLMap()
	defer m.Destroy()

	// Basic Insert
	ret := m.Insert(1, 100)
	t.Logf("insert(1,100) = %d", ret)

	// Get existing key
	code, val := m.Get(1)
	t.Logf("get(1) = code:%d val:%d", code, val)

	// Get non-existent key
	code, _ = m.Get(99)
	t.Logf("get(99) code = %d", code)

	// Delete existing key
	ret = m.Delete(1)
	t.Logf("delete(1) = %d", ret)

	// Double delete
	ret = m.Delete(1)
	t.Logf("delete(1) again = %d", ret)

	// Overwrite existing key
	m.Insert(2, 200)
	ret = m.Insert(2, 300)
	t.Logf("insert(2,300) update ret = %d", ret)
	_, val = m.Get(2)
	t.Logf("get(2) after update = %d", val)
}

func TestLinearizabilitySuites(t *testing.T) {
	testCases := []struct {
		name     string
		threads  int
		keyRange uint64
		ops      int
		scanLen  int
		weights  map[int]int
	}{
		{
			name:    "High_Contention",
			threads: 8, keyRange: 50, ops: 500, scanLen: 0,
			weights: map[int]int{opInsert: 40, opDelete: 20, opGet: 40, opScan: 0},
		},
		{
			name:    "Split_Scan",
			threads: 8, keyRange: 2000, ops: 500, scanLen: 10,
			weights: map[int]int{opInsert: 30, opDelete: 10, opGet: 10, opScan: 50},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for round := 0; round < 10; round++ {
				m := NewBSLMap()
				ops := runStressTest(m, tc.threads, tc.ops, tc.keyRange, tc.scanLen, tc.weights)
				m.Destroy()

				res, _ := porcupine.CheckOperationsVerbose(bslModel, ops, 20*time.Second)
				if res == porcupine.Illegal {
					t.Errorf("[%s] Round %d: FAILED LINEARIZABILITY", tc.name, round)
					return
				}
			}
		})
	}
}
