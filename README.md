## Tomato-redis

---


hello,2022 年春天,我参加了字节镜像计划中的大数据项目,MIT-6.824,测评满分 75 分,我获得了 70 分.

这是我在自学MIT-6.824后,基于实验中的raft框架代码开发的一个分布式的存储系统.

---

### 目前的使用场景:

勤奋蜂官网的 KV 存储

---

### 目前支持功能:

+ [x] Raft 自动选主

+ [x] Raft 日志压缩

+ [x] 多机器部署

+ [x] PUT,GET,SYNC_GET指令

  TODO List

+ [ ] 更加丰富的接口与数据结构

+ [ ] 硬盘持久化

+ [ ] 存储引擎Bitcask

+ [ ] 启动指令

+ [ ] 发布订阅功能

### 分支介绍

---

raft分支是raft框架代码

memoryraftkv分支是基于内存的分布式存储系统,维护是go map




代码结构:

db:存储结构层

raft:共识协议层

kvraft:状态机层

myrpc:rpc的封装

test:测试代码



运行方法(具体细节可以看shell 脚本)

1. bash run-server.sh # 启动服务器

2. bash run-client.sh # 启动客户端

EXAMPLE

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