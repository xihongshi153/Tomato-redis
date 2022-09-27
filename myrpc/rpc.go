package myrpc

import (
	"log"
	"net/rpc"
)

type ClientEnd struct {
	Client *rpc.Client
}

func (c *ClientEnd) Call(function string, arg interface{}, reply interface{}) bool {
	e := c.Client.Call(function, arg, reply)
	if e != nil {
		log.Printf(" c.Client.Call(function, arg, reply) err %v", e)
		return false
	}
	return true
}
