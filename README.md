## Tomato-redis

hello,这是我根据MIT-6.824-LAB-3书写的raft框架代码开发的一个分布式的存储系统
raft分支是raft框架代码
memoryraftkv分支是基于内存的分布式存储系统,维护是go map
bitcaskraftkv(TODO)分支基于bitcask实现db层



代码结构:

db:存储结构层

raft:共识协议层

kvraft:状态机层

myrpc:rpc的封装

test:测试代码



运行方法

1. bash run-server.sh

2. bash run-client.sh

输入例子

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

问题记录:

1. 不知道是硬件的原因还是算法的问题,当访问的协程超过10个的时候,读写不均衡,2000读写,最快完成与最慢完成的时间差增加
   使用16核服务器 2000读写 读写比1:1

| 协程数量 | 50   | 60   | 70   | 80   | 90   |
| -------- | ---- | ---- | ---- | ---- | ---- |
| 花费时间 | 30s  | 34s  | 36s  | 43s  | 46s  |

| 100  | 110  | 120  | 130  | 140  |
| ---- | ---- | ---- | ---- | ---- |
| 50s  | 65s  | 93s  | 93s  | 108s |

| 150  | 200  |      |      |      |
| ---- | ---- | ---- | ---- | ---- |
| 110s | 200s |      |      |      |

​	查pprof看了CPU图,看不出什么问题来....发现rpc,反射解析时间挺长的,尝试用grpc,看看会怎么样