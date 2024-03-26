package consistenthash

import (
	"hash/crc32"
	"testing"
)

func TestHashRing(t *testing.T) {
	// 创建一个新的 HashRing
	ring := New(100, nil, nil)

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
	err = ring.Reset("node1", "node2", "node3", "node4")
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
	err = ring.ResetAll(200, crc32.ChecksumIEEE, crc32.ChecksumIEEE, "node1", "node2", "node3")
	if err != nil {
		t.Errorf("ResetAll nodes failed: %v", err)
	}

	// 测试 Get 方法
	node, err = ring.Get("key1")
	if err != nil {
		t.Errorf("Get node failed: %v", err)
	}
	t.Logf("Node for key1 after resetting all nodes: %s", node)
}
