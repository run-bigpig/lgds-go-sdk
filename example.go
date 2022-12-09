package main

import (
	"github.com/runbig-pig/lgds-go-sdk/lgds"
	"log"
	"os"
)

func main() {
	serverUrl := "https://datalog-v2.66yytx.com/logagent"
	appId := "xxx"
	ak := "xxx"
	sk := "bxxxx"
	comsumer, err := lgds.NewConsumer(serverUrl, appId, ak, sk, true)
	if err != nil {
		log.Println(err)
		os.Exit(0)
	}
	l := lgds.New(comsumer)
	defer l.Close()
	for i := 1; i <= 125; i++ {
		l.Track("123", "456", "app", "ios", "login", i, map[string]interface{}{"level": i})
	}
	l.User("123", "456", "app", "ios", 1, map[string]interface{}{"account": "xafdsafas"})
	for {

	}
}
