package main

import (
	"log"
	"os"
	"strconv"
	"time"
)

const (
	NodeCount   = 8
	ClientCount = 200
	MsgSize     = 1
)

var Clients []*Client

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
	for i := 0; i < ClientCount; i++ {
		Clients[i] = initClient()
		time := Clients[i].EndTime.UnixNano() - Clients[i].StartTime.UnixNano()
		debugLog.Printf("time consume : %v", float64(time)/1000000)
	}

	//gocron.Every(1).Second().Do(initClient)
	//go func() {
	//	<-gocron.Start()
	//}()
}

func initClient() *Client {
	client := NewClient()
	client.Start()
	return client
}
