本一致性哈希库支持自定义虚拟节点副本数，支持自定义哈希函数，对节点和数据进行哈希的哈希函数可以设置为不同，支持添加删除节点或重置节点。
使用说明：
1、使用之前你需要用New函数创建一个hashring，参数有虚拟节点副本数和node和数据key的哈希函数，哈希函数
2、添加hash环上的node，如果有动态更新的话，可以添加和删除
3、通过Get方法获取一个数据对应到hash环上的哪个节点处理
样例代码可以参考test里面的代码。

This consistent hashing library supports custom virtual node replication, custom hash functions, and different hash functions for nodes and data. It allows adding, deleting, or resetting nodes.

Usage Instructions:

Before using, you need to create a hash ring using the New function. The parameters include the number of virtual node replicas and the hash functions for nodes and data keys.
Add nodes to the hash ring. If there are dynamic updates, nodes can be added and deleted.
Use the Get method to determine which node on the hash ring a piece of data corresponds to.
For sample code, please refer to the tests in the codebase.
