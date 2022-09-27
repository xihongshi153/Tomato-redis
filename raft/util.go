package raft

import (
	"log"
	"os"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		file := "./" + "log_raft" + ".txt"
		logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
		if err != nil {
			panic(err)
		}

		log.SetOutput(logFile) // 将文件设置为log输出的文件
		// log.Printf("%v", time.Now())
		log.Printf(format, a...)
	}

	return
}
