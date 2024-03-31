本一致性哈希库支持自定义虚拟节点副本数，支持自定义哈希函数，支持添加删除节点或重置节点。
使用说明：
1、使用之前你需要用New函数创建一个hashring，参数有虚拟节点副本数和哈希函数
2、添加hash环上的node，如果有动态更新的话，可以添加和删除
3、通过Get方法获取一个数据对应到hash环上的哪个节点处理
样例代码可以参考test里面的代码。

The provided consistent hashing library supports customizing the number of virtual node replicas, defining custom hash functions, and adding, deleting, or resetting nodes. Usage instructions:

1. Before using, create a hashring with the New function. The parameters for this function include the number of virtual node replicas and the desired hash function.
2. Add nodes to the hash ring. If there are dynamic updates, nodes can be added and removed.
3. Use the Get method to determine which node on the hash ring is responsible for processing a specific piece of data. Refer to the sample code in the tests for more details.
