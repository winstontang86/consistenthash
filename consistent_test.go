package consistenthash_test

import (
	"fmt"
	"testing"

	"consistenthash"

	"github.com/stretchr/testify/assert"
)

func TestHashRing(t *testing.T) {
	ring := consistenthash.New(100, nil, nil) // 创建新的哈希环，使用100个虚拟节点副本，使用默认值的哈希函数

	err := ring.Add("node1", "node2", "node3")
	assert.NoError(t, err) // 检查是否没有错误

	// 测试 Get 方法
	node, err := ring.Get("apple")
	assert.NoError(t, err) // 检查是否没有错误
	fmt.Printf("apple belongs to %s\n", node)

	// 删除节点
	ring.Remove("node3")

	// 测试 Get 方法
	node, err = ring.Get("apple")
	assert.NoError(t, err) // 检查是否没有错误
	fmt.Printf("apple (after remove) belongs to %s\n", node)

	// 测试 Reset 方法
	err = ring.Reset("node1", "node2", "node3", "node4")
	assert.NoError(t, err) // 检查是否没有错误

	node, err = ring.Get("apple")
	assert.NoError(t, err) // 检查是否没有错误
	fmt.Printf("apple (after reset) belongs to %s\n", node)

	// 测试 ResetAll 方法
	err = ring.ResetAll(
		50,  // 使用 50 个虚拟节点副本
		nil, // 哈希函数保持不变
		nil, // 哈希函数保持不变
		"node1", "node2", "node3", "node4", "node5",
	)
	assert.NoError(t, err) // 检查是否没有错误

	node, err = ring.Get("apple")
	assert.NoError(t, err) // 检查是否没有错误
	fmt.Printf("apple (after reset all) belongs to %s\n", node)
}
