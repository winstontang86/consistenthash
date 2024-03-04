/*
Copyright 2024 winstontang

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package consistenthash provides a flexible hash ring.
// 支持自定义虚拟节点副本数，支持自定义哈希函数，支持添加删除节点的一致性hash库。
package consistenthash

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

var (
	limitVNodes    = 0x1 << 30
	ErrEmptyRing   = errors.New("empty hashring")
	ErrHashRepeat  = errors.New("hashe repeat")
	ErrTooManyNode = errors.New("too many nodes")
)

// implement for sorting
type U32Slice []uint32

// Len returns the length of the uints array.
func (x U32Slice) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x U32Slice) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x U32Slice) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

type Hash32 func(data []byte) uint32

type HashRing struct {
	keyHash    Hash32            // hash for key. Usually the same as nodeHash
	nodeHash   Hash32            // hash for node. Usually the same as keyHash
	replicas   uint16            // number of virtual nodes per physical node
	ring       map[uint32]string // hash ring
	nodes      map[string]bool   // all physical nodes
	nodeCount  int               // physical node count
	vNodeHashs U32Slice          // Sorted virtual node hash32
	sync.RWMutex
}

// New creates a new hash ring.
func New(replicas uint16, keyFn, nodeFn Hash32) *HashRing {
	m := &HashRing{
		keyHash:    keyFn,
		nodeHash:   nodeFn,
		replicas:   replicas,
		ring:       make(map[uint32]string),
		nodes:      make(map[string]bool),
		nodeCount:  0,
		vNodeHashs: make([]uint32, 0),
	}
	if m.keyHash == nil {
		m.keyHash = crc32.ChecksumIEEE
	}
	if m.nodeHash == nil {
		m.nodeHash = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns true if there are no nodes available.
func (m *HashRing) IsEmpty() bool {
	return len(m.ring) == 0
}

// RingInfo information about the hash ring.
func (m *HashRing) RingInfo() string {
	m.RLock()
	defer m.RUnlock()

	s := "HashRing: \n"
	for k, v := range m.ring {
		s += fmt.Sprintf("%d: %s, ", k, v)
	}
	return s
}

// Add adds some nodes to the hashring.
// If return error，MUST reset hashring!
func (m *HashRing) Add(nodes ...string) error {
	m.Lock()
	defer m.Unlock()
	//too much nodes, return error
	if (m.nodeCount+len(nodes))*int(m.replicas) > limitVNodes {
		return ErrTooManyNode
	}
	m.add(nodes...)
	// recheck
	if m.nodeCount*int(m.replicas) != len(m.ring) {
		return ErrHashRepeat
	}
	return nil
}

// need Lock() before calling
func (m *HashRing) add(nodes ...string) {
	for _, node := range nodes {
		// ignored duplicate nodes
		if _, ok := m.nodes[node]; ok {
			continue
		}
		for i := 0; i < int(m.replicas); i++ {
			vhash := m.nodeHash([]byte(m.nodeVKey(node, i)))
			m.ring[vhash] = node
			m.vNodeHashs = append(m.vNodeHashs, vhash)
		}
		m.nodes[node] = true
		m.nodeCount++
	}
	sort.Sort(m.vNodeHashs)
}

// nodeVKey generates a string key for an vnode with an index.
func (m *HashRing) nodeVKey(node string, idx int) string {
	return strconv.Itoa(idx) + node
}

// Remove removes some nodes from the hash.
func (m *HashRing) Remove(node string) {
	m.Lock()
	defer m.Unlock()
	m.remove(node)
}

// need Lock() before calling
func (m *HashRing) remove(nodes ...string) {
	for _, node := range nodes {
		// 检查是否存在，不存在则跳过
		if _, ok := m.nodes[node]; !ok {
			continue
		}
		for i := 0; i < int(m.replicas); i++ {
			vhash := m.nodeHash([]byte(m.nodeVKey(node, i)))
			delete(m.ring, vhash)
		}
		delete(m.nodes, node) //m.nodes[node] = false
		m.nodeCount--
	}
	m.rebuildVNodeSlice()
}

// rebuildVNodeSlice
// need Lock() before calling
func (m *HashRing) rebuildVNodeSlice() {
	// 理论上节点的删除是一个小概率事件，所以这里暂不对内存做优化
	m.vNodeHashs = make([]uint32, 0)
	for k := range m.ring {
		m.vNodeHashs = append(m.vNodeHashs, k)
	}
	sort.Sort(m.vNodeHashs)
}

// Reset reset nodes
// 如果返回了error，则需要resetall，特别是hash函数
func (m *HashRing) Reset(nodes ...string) error {
	m.Lock()
	defer m.Unlock()
	if len(nodes)*int(m.replicas) > limitVNodes {
		return ErrTooManyNode
	}
	// 对于环上的每一个，看是否在reset列表里面——删除
	delNodes := make([]string, 0)
	for ringNode := range m.nodes {
		found := false
		for _, node := range nodes {
			if ringNode == node {
				found = true
				break
			}
		}
		if !found {
			delNodes = append(delNodes, ringNode)
		}
	}
	m.remove(delNodes...)
	// 对于reset列表里面的，看是否在环上——添加
	addNodes := make([]string, 0)
	for _, node := range nodes {
		_, exists := m.nodes[node]
		if exists {
			continue
		} else {
			addNodes = append(addNodes, node)
		}
	}
	m.add(addNodes...)
	// 检查是否有hash重复的虚拟节点
	if m.nodeCount*int(m.replicas) != len(m.ring) {
		return ErrHashRepeat
	}
	return nil
}

// need Lock() before calling
func (m *HashRing) clear() {
	m.nodeHash = nil
	m.keyHash = nil
	m.replicas = 100
	m.ring = make(map[uint32]string)
	m.nodes = make(map[string]bool)
	m.nodeCount = 0
	m.vNodeHashs = make([]uint32, 0)
}

// ResetAll
func (m *HashRing) ResetAll(replicas uint16, keyFn, nodeFn Hash32, nodes ...string) error {
	m.Lock()
	defer m.Unlock()
	// clear and reset hash functions
	m.clear()
	m.replicas = replicas
	m.keyHash = keyFn
	m.nodeHash = nodeFn
	if m.keyHash == nil {
		m.keyHash = crc32.ChecksumIEEE
	}
	if m.nodeHash == nil {
		m.nodeHash = crc32.ChecksumIEEE
	}
	//too much nodes, return error
	if (m.nodeCount+len(nodes))*int(m.replicas) > limitVNodes {
		return ErrTooManyNode
	}
	m.add(nodes...)
	// 检查是否有hash重复的虚拟节点
	if m.nodeCount*int(m.replicas) != len(m.ring) {
		return ErrHashRepeat
	}
	return nil
}

// Get gets the closest node in the hashring to the provided key.
func (m *HashRing) Get(key string) (string, error) {
	if m.IsEmpty() {
		return "", ErrEmptyRing
	}
	hash := m.keyHash([]byte(key))
	// Binary search for appropriate replica.
	idx := sort.Search(len(m.vNodeHashs), func(i int) bool { return m.vNodeHashs[i] >= hash })

	// Attention
	if idx == len(m.vNodeHashs) {
		idx = 0
	}

	return m.ring[m.vNodeHashs[idx]], nil
}
