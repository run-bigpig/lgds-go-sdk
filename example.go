package main

import (
	"github.com/runbig-pig/lgds-go-sdk/lgds"
	"log"
	"os"
)

func main() {
	serverUrl := "https://datalog-v2.66yytx.com/logagent"
	appId := "5649497781"
	ak := "3071s8r9g3rlP15lha8b2be3T"
	sk := "brz2r1nic93gco8mDJ1r55w29"
	comsumer, err := lgds.NewConsumer(serverUrl, appId, ak, sk, true)
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}
	l := lgds.New(comsumer)
	defer l.Close()
	l.Track("123", "456", "app", "ios", "login", 1, map[string]interface{}{"level": 1})
	l.User("123", "456", "app", "ios", 1, map[string]interface{}{"account": "xafdsafas"})
	for {

	}
}
