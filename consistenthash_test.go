package consistenthash

import (
	"hash/crc32"
	"strconv"
	"testing"
	"time"
)

func TestHashRing(t *testing.T) {
	// 创建一个新的 HashRing
	ring := New(100, nil)

	// 添加节点
	err := ring.Add("node1", "node2", "node3")
	if err != nil {
		t.Errorf("Add nodes failed: %v", err)
	}

	// 测试 Get 方法
	node, err := ring.Get("key1")
	if err != nil {
		t.Errorf("Get node failed: %v", err)
	}
	t.Logf("Node for key1: %s", node)

	// 删除节点
	ring.Remove("node1")

	// 测试 Get 方法
	node, err = ring.Get("key1")
	if err != nil {
		t.Errorf("Get node failed: %v", err)
	}
	t.Logf("Node for key1 after removing node1: %s", node)

	// 重置节点
	nodeSet := make(NodeSet)
	nodeSet["node1"] = struct{}{}
	nodeSet["node2"] = struct{}{}
	nodeSet["node3"] = struct{}{}
	nodeSet["node4"] = struct{}{}

	err = ring.Reset(nodeSet)
	if err != nil {
		t.Errorf("Reset nodes failed: %v", err)
	}

	// 测试 Get 方法
	node, err = ring.Get("key1")
	if err != nil {
		t.Errorf("Get node failed: %v", err)
	}
	t.Logf("Node for key1 after resetting nodes: %s", node)

	// 重置所有节点
	err = ring.ResetAll(200, crc32.ChecksumIEEE, "node1", "node2", "node3")
	if err != nil {
		t.Errorf("ResetAll nodes failed: %v", err)
	}
	err = ring.ResetAll(0, nil, "node1", "node2", "node5")
	if err != nil {
		t.Errorf("ResetAll nodes failed: %v", err)
	}

	// 测试 Get 方法
	node, err = ring.Get("key1")
	if err != nil {
		t.Errorf("Get node failed: %v", err)
	}
	t.Logf("Node for key1 after resetting all nodes: %s", node)

	// 测试 RingInfo 方法
	t.Logf("RingInfo: %s", ring.RingInfo())
}

const (
	numNodes = 100
)

func BenchmarkGet(b *testing.B) {
	hashRing := New(100, nil)

	// 添加初始节点
	initialNodes := make(NodeSet, numNodes)
	for i := 0; i < numNodes; i++ {
		key := "node" + strconv.Itoa(i)
		initialNodes[key] = struct{}{}
	}
	err := hashRing.Reset(initialNodes)
	if err != nil {
		b.Errorf("Error resetting nodes: %v", err)
	}

	// 测试主循环
	for ti := 0; ti < b.N; ti++ {
		key := string("key_") + strconv.Itoa(int(time.Now().UnixMicro()))
		_, err := hashRing.Get(key)
		if err != nil {
			b.Fatalf("Error getting node for key: %v", err)
		}
	}
}

// BenchmarkReset0 - 测试 Reset方法的性能，每次节点不变
func BenchmarkReset0(b *testing.B) {
	hashRing := New(100, nil)

	// 添加初始节点
	initialNodes := make(NodeSet, numNodes)
	i := 0
	for ; i < numNodes; i++ {
		key := "node" + strconv.Itoa(i)
		initialNodes[key] = struct{}{}
	}
	// 测试主循环
	for ti := 0; ti < b.N; ti++ {
		err := hashRing.Reset(initialNodes)
		if err != nil {
			b.Errorf("Error resetting nodes: %v", err)
		}
	}
}

// BenchmarkReset - 测试 Reset方法的性能，每次节点变化一个
func BenchmarkReset(b *testing.B) {
	hashRing := New(100, nil)

	// 添加初始节点
	initialNodes := make(NodeSet, numNodes)
	i := 0
	for ; i < numNodes; i++ {
		key := "node" + strconv.Itoa(i)
		initialNodes[key] = struct{}{}
	}
	// 测试主循环
	for ti := 0; ti < b.N; ti++ {
		err := hashRing.Reset(initialNodes)
		if err != nil {
			b.Errorf("Error resetting nodes: %v", err)
		}
		key := "node" + strconv.Itoa(i)
		delete(initialNodes, key)
		i++
		key = "node" + strconv.Itoa(i)
		initialNodes[key] = struct{}{}
	}
}

func BenchmarkResetAll(b *testing.B) {
	hashRing := New(100, nil)

	// 添加初始节点
	nodes := make([]string, numNodes)
	i := 0
	for ; i < numNodes; i++ {
		key := "node" + strconv.Itoa(i)
		nodes = append(nodes, key)
	}
	// 测试主循环
	for ti := 0; ti < b.N; ti++ {
		err := hashRing.ResetAll(100, nil, nodes...)
		if err != nil {
			b.Errorf("Error resetting nodes: %v", err)
		}
	}
}

func BenchmarkRingInfo(b *testing.B) {
	ringInfo := ""
	hashRing := New(100, nil)

	// 添加初始节点
	nodes := make([]string, numNodes)
	i := 0
	for ; i < numNodes; i++ {
		key := "node" + strconv.Itoa(i)
		nodes = append(nodes, key)
	}
	err := hashRing.Add(nodes...)
	if err != nil {
		b.Errorf("Error add nodes: %v", err)
	}
	// 测试主循环
	for ti := 0; ti < b.N; ti++ {
		ringInfo = hashRing.RingInfo()
	}
	if err != nil {
		b.Errorf("RingInfo: %s", ringInfo)
	}
}
