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
	"hash/crc32"
	"math"
	"sort"
	"strconv"
	"sync"
)

const (
	// virtual node count limit
	limitVNodes            = 0x1 << 30
	miniReplicas    uint16 = 2
	defaultReplicas uint16 = 128
	// init capacity for virtual node
	initVNodeCap = 2048
	// init capacity for physical node
	initPNodeCap = 16 // 2048 / 128 = 16
)

var (
	// ErrRingEmpty "ring empty"
	ErrRingEmpty = errors.New("ring empty")
	// ErrRingFull "ring full"
	ErrRingFull = errors.New("ring full")
)

// U32Slice implement for sorting
// 实现sort.Interface接口的Uint32Slice
type U32Slice []uint32

// Len returns the length of the uints array.
func (x U32Slice) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x U32Slice) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x U32Slice) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// Hash32 func with uint32 return
// 输出为uint32的hash函数
type Hash32 func(data []byte) uint32

// HashRing core struct for hashring
// 一致性hash环的结构体
type HashRing struct {
	HashFunc    Hash32              // hash func for key and for node
	replicas    uint16              // virtual nodes per physical, MUST greater than 2
	vNodes      U32Slice            // Sorted virtual node hash32
	vnodeToNode map[uint32]string   // hash ring
	nodeToVnode map[string]U32Slice // all physical nodes. Slice for delete performence

	sync.RWMutex
}

// New creates a new hash ring. With default hash function crc32.
// 创建hash环，默认hash函数为crc32.ChecksumIEEE
func New(replicas uint16, hash Hash32) *HashRing {
	hintCap := initVNodeCap
	if int(replicas) > hintCap {
		hintCap = int(replicas)
	}

	m := &HashRing{
		HashFunc:    hash,
		replicas:    replicas,
		vNodes:      make([]uint32, 0, hintCap),
		vnodeToNode: make(map[uint32]string, hintCap),
		nodeToVnode: make(map[string]U32Slice, initPNodeCap),
	}
	// 强制修正错误输入
	if m.replicas < miniReplicas {
		m.replicas = defaultReplicas
	}
	if m.HashFunc == nil {
		m.HashFunc = crc32.ChecksumIEEE
	}
	return m
}

// numHash big uint32 to small uint32
// 把大整数hash到一个小整数
func numHash(key, max uint32) uint32 {
	const prime uint32 = 16777619
	// 乘法有可能溢出导致环绕，但不影响逻辑
	return (key * prime) % max
}

// combKey generates a string key for an vnode with an index.
// 字符串和index组合成一个key
func combKey(node string, idx int) string {
	return strconv.Itoa(idx) + node
}

// IsEmpty 是否空
func (m *HashRing) IsEmpty() bool {
	m.RLock()
	defer m.RUnlock()

	return len(m.vnodeToNode) == 0
}

// RingInfo information about the hash ring.
func (m *HashRing) RingInfo() string {
	m.RLock()
	defer m.RUnlock()

	s := "HashRing: vnode count = " + strconv.Itoa(len(m.vnodeToNode)) +
		" node count = , " + strconv.Itoa(len(m.nodeToVnode))

	return s
}

// Add adds some nodes to the hashring.
// If return error，MUST ResetAll hashring, typically by adjusting the replicas!
// 返回错误，必须接收和处理
func (m *HashRing) Add(nodes ...string) error {
	m.Lock()
	defer m.Unlock()
	// too much nodes, return error
	if len(m.vnodeToNode)+len(nodes)*int(m.replicas) > limitVNodes {
		return ErrRingFull
	}
	m.add(true, nodes...)
	return nil
}

// add MUST Lock() before called
// The doSort CANNOT be false, unless you are SURE DO THE SORT.
func (m *HashRing) add(doSort bool, nodes ...string) {
	if len(nodes) == 0 {
		return
	}
	segmentLen := uint32(math.MaxUint32 / int(m.replicas))
	for _, node := range nodes {
		// Ignored duplicate node
		if _, ok := m.nodeToVnode[node]; ok {
			continue
		}
		// Add physical node
		m.nodeToVnode[node] = make(U32Slice, 0, m.replicas)
		// add vitual nodes
		for ui := uint16(0); ui < m.replicas; ui++ {
			segmentStart := segmentLen * uint32(ui)
			vhash32 := m.HashFunc([]byte(combKey(node, int(ui))))
			segmentIdx := numHash(vhash32, segmentLen)
			vhash32 = segmentStart + segmentIdx
			// 检查是否有hash冲突，有冲突重hash两次
			if _, ok := m.vnodeToNode[vhash32]; ok {
				segmentIdx = numHash(vhash32+1, segmentLen)
				vhash32 = segmentStart + segmentIdx
				if _, ok := m.vnodeToNode[vhash32]; ok {
					segmentIdx = numHash(vhash32+1, segmentLen)
					vhash32 = segmentStart + segmentIdx
					if _, ok := m.vnodeToNode[vhash32]; ok {
						// 如果还是冲突则直接跳过，逻辑无影响，稍微对均衡性有影响
						continue
					}
				}
			}
			m.vnodeToNode[vhash32] = node
			m.vNodes = append(m.vNodes, vhash32)
			m.nodeToVnode[node] = append(m.nodeToVnode[node], vhash32)
		}
	}
	if doSort {
		sort.Sort(m.vNodes)
	}
}

// Remove removes some nodes from the hash.
// 删除节点
func (m *HashRing) Remove(nodes ...string) {
	m.Lock()
	defer m.Unlock()
	m.remove(true, nodes...)
}

// remove MUST Lock() before calling
// The doSort CANNOT be false, unless you are SURE DO THE SORT.
func (m *HashRing) remove(doSort bool, nodes ...string) {
	if len(nodes) == 0 {
		return
	}
	for _, node := range nodes {
		// 检查是否存在，不存在则跳过
		if _, ok := m.nodeToVnode[node]; !ok {
			continue
		}
		// 删除虚拟节点和映射关系
		for _, vhash32 := range m.nodeToVnode[node] {
			delete(m.vnodeToNode, vhash32)
		}
		delete(m.nodeToVnode, node)
	}
	m.rebuildVNodeSlice(doSort)
}

// rebuildVNodeSlice 重建虚拟节点切片
// MUST Lock() before called, doSort usually true
func (m *HashRing) rebuildVNodeSlice(doSort bool) {
	// 直接复用现有内存，不新开辟内存
	m.vNodes = m.vNodes[0:0]
	for k := range m.vnodeToNode {
		m.vNodes = append(m.vNodes, k)
	}
	if doSort {
		sort.Sort(m.vNodes)
	}
}

// Reset reset nodes
// If return error，MUST ResetAll hashring, typically by adjusting the replicas!
// 返回错误，必须接收和处理
func (m *HashRing) Reset(nodes ...string) error {
	m.Lock()
	defer m.Unlock()
	if len(nodes)*int(m.replicas) > limitVNodes {
		return ErrRingFull
	}
	resetNodes := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		resetNodes[node] = struct{}{}
	}
	// 遍历物理节点，看是否在reset列表里面，不在的删除
	delNodes := make([]string, 0)
	for ringNode := range m.nodeToVnode {
		if _, exists := resetNodes[ringNode]; !exists {
			delNodes = append(delNodes, ringNode)
		}
	}
	// 只做一次排序，所以remove和add里面不排序
	m.remove(false, delNodes...)
	// 对于reset列表里面的，看是否在环上，不在的添加
	addNodes := make([]string, 0)
	for node := range resetNodes {
		if _, exists := m.nodeToVnode[node]; !exists {
			addNodes = append(addNodes, node)
		}
	}
	// 只做一次排序，所以remove和add里面不排序
	m.add(false, addNodes...)
	// MUST DO sort
	if len(delNodes) > 0 || len(addNodes) > 0 {
		sort.Sort(m.vNodes)
	}

	return nil
}

// clear clear hashring
// MUST Lock() before called
func (m *HashRing) clear() {
	m.HashFunc = crc32.ChecksumIEEE
	m.replicas = defaultReplicas
	m.vNodes = make([]uint32, 0, initVNodeCap)
	m.vnodeToNode = make(map[uint32]string, initVNodeCap)
	m.nodeToVnode = make(map[string]U32Slice, initPNodeCap)
}

// ResetAll 重置
func (m *HashRing) ResetAll(replicas uint16, hash Hash32, nodes ...string) error {
	m.Lock()
	defer m.Unlock()
	// clear and reset hash functions
	m.clear()
	if replicas >= miniReplicas {
		m.replicas = replicas
	}
	if hash != nil {
		m.HashFunc = hash
	}
	// too much nodes, return error
	if len(nodes)*int(m.replicas) > limitVNodes {
		return ErrRingFull
	}
	m.add(true, nodes...)

	return nil
}

// Get gets the closest node in the hashring to the provided key.
// 获取key对应的节点
func (m *HashRing) Get(key string) (string, error) {
	m.RLock()
	defer m.RUnlock()

	if len(m.vnodeToNode) == 0 {
		return "", ErrRingEmpty
	}

	u32Hash := m.HashFunc([]byte(key))
	// Binary search for appropriate replica.
	idx := sort.Search(len(m.vNodes), func(i int) bool { return m.vNodes[i] >= u32Hash })

	// Attention
	if idx == len(m.vNodes) {
		idx = 0
	}

	return m.vnodeToNode[m.vNodes[idx]], nil
}
