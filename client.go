package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	nodeId     int
	url        string
	keypair    Keypair
	knownNodes []*KnownNode
	request    *RequestMsg
	replyLog   map[int]*ReplyMsg
	mutex      sync.Mutex
	StartTime  time.Time
	EndTime    time.Time
}

func NewClient() *Client {
	client := &Client{
		nodeId:     ClientNode.nodeID,
		url:        ClientNode.url,
		keypair:    KeypairMap[ClientNode.nodeID],
		knownNodes: KnownNodes,
		request:    nil,
		replyLog:   make(map[int]*ReplyMsg),
		mutex:      sync.Mutex{},
	}
	return client
}

func (c *Client) Start() {
	c.sendRequest()
	ln, err := net.Listen("tcp", c.url)
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		reply := c.handleConnection(conn)
		if reply {
			break
		}
	}
}

func (c *Client) handleConnection(conn net.Conn) (reply bool) {
	req, err := ioutil.ReadAll(conn)
	header, payload, _ := SplitMsg(req)
	if err != nil {
		panic(err)
	}
	switch header {
	case hReply:
		reply = c.handleReply(payload)
	}
	return reply
}

func (c *Client) sendRequest() {
	var buffer bytes.Buffer
	for i := 0; i < MsgSize*1024; i++ {
		buffer.WriteString(strconv.Itoa(rand.Intn(10)))
	}
	msg := buffer.String()
	req := Request{
		msg,
		hex.EncodeToString(generateDigest(msg)),
	}
	reqmsg := &RequestMsg{
		"solve",
		int(time.Now().Unix()),
		c.nodeId,
		req,
	}
	sig, err := c.signMessage(reqmsg)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	logBroadcastMsg(hRequest, reqmsg)
	c.StartTime = time.Now()
	send(ComposeMsg(hRequest, reqmsg, sig), c.findPrimaryNode().url)
	c.request = reqmsg
}

func (c *Client) handleReply(payload []byte) bool {
	var replyMsg ReplyMsg
	err := json.Unmarshal(payload, &replyMsg)
	if err != nil {
		fmt.Printf("error happened:%v", err)
		return false
	}
	logHandleMsg(hReply, replyMsg, replyMsg.NodeID)
	c.mutex.Lock()
	c.replyLog[replyMsg.NodeID] = &replyMsg
	rlen := len(c.replyLog)
	c.mutex.Unlock()
	if rlen >= c.countNeedReceiveMsgAmount() {
		fmt.Println("request success!!")
		c.EndTime = time.Now()
		return true
	}
	return false
}

func (c *Client) signMessage(msg interface{}) ([]byte, error) {
	sig, err := signMessage(msg, c.keypair.privkey)
	if err != nil {
		return nil, err
	}
	return sig, nil
}

func (c *Client) findPrimaryNode() *KnownNode {
	nodeId := ViewID % len(c.knownNodes)
	for _, knownNode := range c.knownNodes {
		if knownNode.nodeID == nodeId {
			return knownNode
		}
	}
	return nil
}

func (c *Client) countTolerateFaultNode() int {
	return (len(c.knownNodes) - 1) / 3
}

func (c *Client) countNeedReceiveMsgAmount() int {
	f := c.countTolerateFaultNode()
	return f + 1
}
