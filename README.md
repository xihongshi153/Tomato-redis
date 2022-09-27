## Tomato-redis
hello,这是我根据MIT-6.824-LAB-3书写的raft框架代码,并基于框架代码,开发的一个分布式的存储系统
raft分支是raft框架代码
memoryraftkv分支是基于内存的分布式存储系统,维护是go map
bitcaskraftkv分支基于bitcask实现db层

运行方法
1. bash run-server.sh
2. bash run-client.sh
输入格式

例子
```
client >put
input key
a
input value
b
client >get
input key
a
b
```
