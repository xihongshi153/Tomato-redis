#!/bin/bash
go run main.go -kind="server" -id=0 -name="peer1" -port="127.0.0.1:8003 127.0.0.1:8001 127.0.0.1:8002" &
go run main.go -kind="server" -id=1 -name="peer2" -port="127.0.0.1:8003 127.0.0.1:8001 127.0.0.1:8002" &
go run main.go -kind="server" -id=2 -name="peer3" -port="127.0.0.1:8003 127.0.0.1:8001 127.0.0.1:8002" &
# #                                         这里输入的一定要是一个空格间隔
# go run main.go -id=0 -name="peer1" -port=":8000 :8001 :8002" &
# go run main.go -id=1 -name="peer2" -port=":8000 :8001 :8002" &
# go run main.go -id=2 -name="peer3" -port=":8000 :8001 :8002" &