package main

import (
	jsoniter "github.com/json-iterator/go"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	NodeCount   = 8
	ClientCount = 200
	MsgSize     = 1
)

var Clients []*Client

var json jsoniter.API

func init() {
	json = jsoniter.ConfigCompatibleWithStandardLibrary
}

func main() {
	//app := &cli.App{
	//	Commands: []*cli.Command{
	//		PBFTCommand,
	//	},
	//}

	//err := app.Run(os.Args)

	//if err != nil {
	//	fmt.Errorf("%s", err)
	//}

	for i := 0; i < NodeCount; i++ {
		server := NewServer(i)
		go server.Start()
	}

	time.Sleep(2 * time.Second)

	fileName := "output/debug.log." + strconv.FormatInt(time.Now().Unix(), 10) //在工程路径下和src同级，也可以写绝对路径，不过要注意转义符
	logFile, err := os.Create(fileName)                                        //创建该文件，返回句柄
	if err != nil {
		log.Fatalln("open file error !")
	}
	debugLog := log.New(logFile, "", log.Llongfile)
	defer logFile.Close() //确保文件在该函数执行完以后关闭

	Clients := make([]*Client, ClientCount)
	wg := sync.WaitGroup{}
	for i := 0; i < ClientCount; i++ {
		wg.Add(1)
		go func(i int32) {
			defer wg.Done()
			client := NewClient(i)
			client.Start()
			Clients[i] = client
		}(int32(i))
	}
	wg.Wait()
	for i := 0; i < ClientCount; i++ {
		time := Clients[i].EndTime.UnixNano() - Clients[i].StartTime.UnixNano()
		debugLog.Printf("time consume : %v", float64(time)/1000000)
	}

	//gocron.Every(1).Second().Do(initClient)
	//go func() {
	//	<-gocron.Start()
	//}()
}
