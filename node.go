package main

import (
	"crypto/rsa"
	"fmt"
	"sync"
	"time"
)

const ViewID = 0

type Node struct {
	NodeID      int
	knownNodes  []*KnownNode
	sequenceID  int
	View        int
	msgQueue    chan []byte
	keypair     Keypair
	msgLog      *MsgLog
	requestPool map[string]*RequestMsg
	mutex       sync.Mutex
}

type Keypair struct {
	privkey *rsa.PrivateKey
	pubkey  *rsa.PublicKey
}

type MsgLog struct {
	preprepareLog map[string]map[int]bool
	prepareLog    map[string]map[int]bool
	commitLog     map[string]map[int]bool
	replyLog      map[string]bool
}

func NewNode(nodeID int) *Node {
	return &Node{
		nodeID,
		KnownNodes,
		0,
		ViewID,
		make(chan []byte),
		KnownKeypairMap[nodeID],
		&MsgLog{
			make(map[string]map[int]bool),
			make(map[string]map[int]bool),
			make(map[string]map[int]bool),
			make(map[string]bool),
		},
		make(map[string]*RequestMsg),
		sync.Mutex{},
	}
}

func (node *Node) getSequenceID() int {
	seq := node.sequenceID
	node.sequenceID++
	return seq
}

func (node *Node) Start() {
	go node.handleMsg()
}

func (node *Node) handleMsg() {
	for {
		msg := <-node.msgQueue
		netMsg := NetMsg{}
		json.Unmarshal(msg, &netMsg)
		//header, payload, sign := SplitMsg(msg)
		switch netMsg.Header {
		case hRequest:
			node.handleRequest(netMsg.RequestMsg, netMsg.Signature, netMsg.ClientNodePubkey, netMsg.ClientUrl)
		case hPrePrepare:
			node.handlePrePrepare(netMsg.PrePrepareMsg, netMsg.Signature, netMsg.ClientUrl)
		case hPrepare:
			node.handlePrepare(netMsg.PrepareMsg, netMsg.Signature, netMsg.ClientUrl)
		case hCommit:
			node.handleCommit(netMsg.CommitMsg, netMsg.Signature, netMsg.ClientUrl)
		}
	}
}

func (node *Node) handleRequest(request *RequestMsg, sig []byte, clientNodePubkey *rsa.PublicKey, clientNodeUrl string) {
	var prePrepareMsg PrePrepareMsg
	//logHandleMsg(hRequest, request, request.ClientID)
	// verify request's digest
	vdig := verifyDigest(request.CRequest.Message, request.CRequest.Digest)
	if vdig == false {
		fmt.Printf("verifyDigest failed\n")
		return
	}
	//verigy request's signature
	_, err := verifySignatrue(request, sig, clientNodePubkey)
	if err != nil {
		fmt.Printf("verify signature failed:%v\n", err)
		return
	}
	node.mutex.Lock()
	node.requestPool[request.CRequest.Digest] = request
	seqID := node.getSequenceID()
	node.mutex.Unlock()
	prePrepareMsg = PrePrepareMsg{
		Request:    *request,
		Digest:     request.CRequest.Digest,
		ViewID:     ViewID,
		SequenceID: seqID,
	}
	//sign prePrepareMsg
	msgSig, err := node.signMessage(prePrepareMsg)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	data := &NetMsg{
		Header:        hPrePrepare,
		PrePrepareMsg: &prePrepareMsg,
		Signature:     msgSig,
		ClientUrl:     clientNodeUrl,
	}
	marshalMsg, _ := json.Marshal(data)
	node.mutex.Lock()
	// put preprepare msg into log
	if node.msgLog.preprepareLog[prePrepareMsg.Digest] == nil {
		node.msgLog.preprepareLog[prePrepareMsg.Digest] = make(map[int]bool)
	}
	node.msgLog.preprepareLog[prePrepareMsg.Digest][node.NodeID] = true
	node.mutex.Unlock()
	//logBroadcastMsg(hPrePrepare, prePrepareMsg)
	node.broadcast(marshalMsg)
}

func (node *Node) handlePrePrepare(prePrepareMsg *PrePrepareMsg, sig []byte, clientNodeUrl string) {
	pnodeId := node.findPrimaryNode()
	//logHandleMsg(hPrePrepare, prePrepareMsg, pnodeId)
	msgPubkey := node.findNodePubkey(pnodeId)
	if msgPubkey == nil {
		fmt.Println("can't find primary node's public key")
		return
	}
	// verify msg's signature
	_, err := verifySignatrue(prePrepareMsg, sig, msgPubkey)
	if err != nil {
		fmt.Printf("verify signature failed:%v\n", err)
		return
	}

	// verify prePrepare's digest is equal to request's digest
	if prePrepareMsg.Digest != prePrepareMsg.Request.CRequest.Digest {
		fmt.Printf("verify digest failed\n")
		return
	}
	node.mutex.Lock()
	node.requestPool[prePrepareMsg.Request.CRequest.Digest] = &prePrepareMsg.Request
	node.mutex.Unlock()
	err = node.verifyRequestDigest(prePrepareMsg.Digest)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	// put preprepare's msg into log
	node.mutex.Lock()
	if node.msgLog.preprepareLog[prePrepareMsg.Digest] == nil {
		node.msgLog.preprepareLog[prePrepareMsg.Digest] = make(map[int]bool)
	}
	node.msgLog.preprepareLog[prePrepareMsg.Digest][pnodeId] = true
	node.mutex.Unlock()
	prepareMsg := PrepareMsg{
		prePrepareMsg.Digest,
		ViewID,
		prePrepareMsg.SequenceID,
		node.NodeID,
	}
	// sign prepare msg
	msgSig, err := signMessage(prepareMsg, node.keypair.privkey)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	data := &NetMsg{
		Header:     hPrepare,
		PrepareMsg: &prepareMsg,
		Signature:  msgSig,
		ClientUrl:  clientNodeUrl,
	}
	marshalMsg, _ := json.Marshal(data)
	node.mutex.Lock()
	// put prepare msg into log
	if node.msgLog.prepareLog[prepareMsg.Digest] == nil {
		node.msgLog.prepareLog[prepareMsg.Digest] = make(map[int]bool)
	}
	node.msgLog.prepareLog[prepareMsg.Digest][node.NodeID] = true
	node.mutex.Unlock()
	//logBroadcastMsg(hPrepare, prepareMsg)
	node.broadcast(marshalMsg)
}

func (node *Node) handlePrepare(prepareMsg *PrepareMsg, sig []byte, clientNodeUrl string) {
	//logHandleMsg(hPrepare, prepareMsg, prepareMsg.NodeID)
	// verify prepareMsg
	pubkey := node.findNodePubkey(prepareMsg.NodeID)
	_, err := verifySignatrue(prepareMsg, sig, pubkey)
	if err != nil {
		fmt.Printf("verify signature failed:%v\n", err)
		return
	}
	// verify request's digest
	err = node.verifyRequestDigest(prepareMsg.Digest)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	// verify prepareMsg's digest is equal to preprepareMsg's digest
	pnodeId := node.findPrimaryNode()
	exist := node.msgLog.preprepareLog[prepareMsg.Digest][pnodeId]
	if !exist {
		fmt.Printf("this digest's preprepare msg by %d not existed\n", pnodeId)
		return
	}
	// put prepareMsg into log
	node.mutex.Lock()
	if node.msgLog.prepareLog[prepareMsg.Digest] == nil {
		node.msgLog.prepareLog[prepareMsg.Digest] = make(map[int]bool)
	}
	node.msgLog.prepareLog[prepareMsg.Digest][prepareMsg.NodeID] = true
	node.mutex.Unlock()
	// if receive prepare msg >= 2f +1, then broadcast commit msg
	limit := node.countNeedReceiveMsgAmount()
	sum, err := node.findVerifiedPrepareMsgCount(prepareMsg.Digest)
	if err != nil {
		fmt.Printf("error happened:%v", err)
		return
	}
	if sum >= limit {
		// if already Send commit msg, then do nothing
		node.mutex.Lock()
		exist, _ := node.msgLog.commitLog[prepareMsg.Digest][node.NodeID]
		node.mutex.Unlock()
		if exist != false {
			return
		}
		//Send commit msg
		commitMsg := CommitMsg{
			prepareMsg.Digest,
			prepareMsg.ViewID,
			prepareMsg.SequenceID,
			node.NodeID,
		}
		sig, err := node.signMessage(commitMsg)
		if err != nil {
			fmt.Printf("sign message happened error:%v\n", err)
		}
		data := &NetMsg{
			Header:    hCommit,
			CommitMsg: &commitMsg,
			Signature: sig,
			ClientUrl: clientNodeUrl,
		}
		marshalMsg, _ := json.Marshal(data)
		// put commit msg to log
		node.mutex.Lock()
		if node.msgLog.commitLog[commitMsg.Digest] == nil {
			node.msgLog.commitLog[commitMsg.Digest] = make(map[int]bool)
		}
		node.msgLog.commitLog[commitMsg.Digest][node.NodeID] = true
		node.mutex.Unlock()
		//logBroadcastMsg(hCommit, commitMsg)
		node.broadcast(marshalMsg)
	}
}

func (node *Node) handleCommit(commitMsg *CommitMsg, sig []byte, clientNodeUrl string) {
	//logHandleMsg(hCommit, commitMsg, commitMsg.NodeID)
	//verify commitMsg's signature
	msgPubKey := node.findNodePubkey(commitMsg.NodeID)
	verify, err := verifySignatrue(commitMsg, sig, msgPubKey)
	if err != nil {
		fmt.Printf("verify signature failed:%v\n", err)
		return
	}
	if verify == false {
		fmt.Printf("verify signature failed\n")
		return
	}
	// verify request's digest
	err = node.verifyRequestDigest(commitMsg.Digest)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	// put commitMsg into log
	node.mutex.Lock()
	if node.msgLog.commitLog[commitMsg.Digest] == nil {
		node.msgLog.commitLog[commitMsg.Digest] = make(map[int]bool)
	}
	node.msgLog.commitLog[commitMsg.Digest][commitMsg.NodeID] = true
	node.mutex.Unlock()
	// if receive commit msg >= 2f +1, then Send reply msg to client
	limit := node.countNeedReceiveMsgAmount()
	sum, err := node.findVerifiedCommitMsgCount(commitMsg.Digest)
	if err != nil {
		fmt.Printf("error happened:%v", err)
		return
	}
	if sum >= limit {
		// if already Send reply msg, then do nothing
		node.mutex.Lock()
		exist := node.msgLog.replyLog[commitMsg.Digest]
		node.mutex.Unlock()
		if exist == true {
			return
		}
		// Send reply msg
		node.mutex.Lock()
		requestMsg := node.requestPool[commitMsg.Digest]
		node.mutex.Unlock()
		//fmt.Printf("operstion:%s  message:%s executed... \n", requestMsg.Operation, requestMsg.CRequest.Message)
		done := fmt.Sprintf("operstion:%s  message:%s done ", requestMsg.Operation, requestMsg.CRequest.Message)
		replyMsg := ReplyMsg{
			node.View,
			int(time.Now().Unix()),
			requestMsg.ClientID,
			node.NodeID,
			done,
		}
		//logBroadcastMsg(hReply, replyMsg)
		Send(ComposeMsg(hReply, replyMsg, []byte{}), clientNodeUrl)
		node.mutex.Lock()
		node.msgLog.replyLog[commitMsg.Digest] = true
		node.mutex.Unlock()
	}
}

func (node *Node) verifyRequestDigest(digest string) error {
	node.mutex.Lock()
	_, ok := node.requestPool[digest]
	if !ok {
		node.mutex.Unlock()
		return fmt.Errorf("verify request digest failed\n")

	}
	node.mutex.Unlock()
	return nil
}

func (node *Node) findVerifiedPrepareMsgCount(digest string) (int, error) {
	sum := 0
	node.mutex.Lock()
	for _, exist := range node.msgLog.prepareLog[digest] {
		if exist == true {
			sum++
		}
	}
	node.mutex.Unlock()
	return sum, nil
}

func (node *Node) findVerifiedCommitMsgCount(digest string) (int, error) {
	sum := 0
	node.mutex.Lock()
	for _, exist := range node.msgLog.commitLog[digest] {

		if exist == true {
			sum++
		}
	}
	node.mutex.Unlock()
	return sum, nil
}

func (node *Node) broadcast(data []byte) {
	for _, knownNode := range node.knownNodes {
		if knownNode.nodeID != node.NodeID {
			err := Send(data, knownNode.url)
			if err != nil {
				fmt.Printf("%v", err)
			}
		}
	}

}

func (node *Node) findNodePubkey(nodeId int) *rsa.PublicKey {
	for _, knownNode := range node.knownNodes {
		if knownNode.nodeID == nodeId {
			return knownNode.pubkey
		}
	}
	return nil
}

func (node *Node) signMessage(msg interface{}) ([]byte, error) {
	sig, err := signMessage(msg, node.keypair.privkey)
	if err != nil {
		return nil, err
	}
	return sig, nil
}

func (node *Node) findPrimaryNode() int {
	return ViewID % len(node.knownNodes)
}

func (node *Node) countTolerateFaultNode() int {
	return (len(node.knownNodes) - 1) / 3
}

func (node *Node) countNeedReceiveMsgAmount() int {
	f := node.countTolerateFaultNode()
	return 2*f + 1
}
