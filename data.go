package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

var (
	privateKeys      []*rsa.PrivateKey
	publicKeys       []*rsa.PublicKey
	privateKeyClient *rsa.PrivateKey
	publicKeyClient  *rsa.PublicKey
	KnownNodes       []*KnownNode
	KeypairMap       map[int]Keypair
	ClientNode       *KnownNode
)

func init() {
	privateKeys = make([]*rsa.PrivateKey, NodeCount)
	publicKeys = make([]*rsa.PublicKey, NodeCount)
	KnownNodes = make([]*KnownNode, NodeCount)
	KeypairMap = make(map[int]Keypair)

	var err error
	generateKeyFiles()
	for i := 0; i < NodeCount; i++ {
		privateKeys[i], publicKeys[i], err = getKeyPairByFile(i)
		if err != nil {
			panic(err)
		}
	}
	privateKeyClient, publicKeyClient, err = getKeyPairByFile(NodeCount)
	if err != nil {
		panic(err)
	}
	for i := 0; i < NodeCount; i++ {
		port := strconv.Itoa(8080 + i)
		KnownNodes[i] = &KnownNode{
			nodeID: i,
			url:    "localhost:" + port,
			pubkey: publicKeys[i],
		}
		KeypairMap[i] = Keypair{
			privateKeys[i],
			publicKeys[i],
		}
	}
	KeypairMap[NodeCount] = Keypair{
		privateKeyClient,
		publicKeyClient,
	}
	port := strconv.Itoa(8080 + NodeCount)
	ClientNode = &KnownNode{
		NodeCount,
		"localhost:" + port,
		publicKeyClient,
	}
}

func getKeyPairByFile(nodeID int) (*rsa.PrivateKey, *rsa.PublicKey, error) {
	privFile, _ := filepath.Abs(fmt.Sprintf("./Keys/%d_priv", nodeID))
	pubFile, _ := filepath.Abs(fmt.Sprintf("./Keys/%d_pub", nodeID))
	fbytes, err := ioutil.ReadFile(privFile)
	if err != nil {
		return nil, nil, err
	}
	block, _ := pem.Decode(fbytes)
	if block == nil {
		return nil, nil, fmt.Errorf("parse block occured error")
	}
	privkey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	pubfbytes, err := ioutil.ReadFile(pubFile)
	if err != nil {
		return nil, nil, err
	}
	pubblock, _ := pem.Decode(pubfbytes)
	if pubblock == nil {
		return nil, nil, fmt.Errorf("parse block occured error")
	}
	pubkey, err := x509.ParsePKIXPublicKey(pubblock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return privkey, pubkey.(*rsa.PublicKey), nil
}

func generateKeyFiles() {
	if !FileExists("./Keys") {
		err := os.Mkdir("Keys", 0700)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i <= NodeCount; i++ {
		filename, _ := filepath.Abs(fmt.Sprintf("./Keys/%d", i))
		if !FileExists(filename+"_priv") && !FileExists(filename+"_pub") {
			priv, pub := generateKeyPair()
			err := ioutil.WriteFile(filename+"_priv", priv, 0644)
			if err != nil {
				panic(err)
			}
			ioutil.WriteFile(filename+"_pub", pub, 0644)
			if err != nil {
				panic(err)
			}
		}
	}
}

func generateKeyPair() ([]byte, []byte) {
	privkey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	mprivkey := x509.MarshalPKCS1PrivateKey(privkey)
	block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: mprivkey,
	}
	bprivkey := pem.EncodeToMemory(block)
	pubkey := &privkey.PublicKey
	mpubkey, err := x509.MarshalPKIXPublicKey(pubkey)
	if err != nil {
		panic(err)
	}
	block = &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: mpubkey,
	}
	bpubkey := pem.EncodeToMemory(block)
	return bprivkey, bpubkey
}
