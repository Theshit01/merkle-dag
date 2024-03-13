package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
	Type  NodeType
}

type NodeType int

const (
	FILE NodeType = iota
	DIR
)

type Node interface {
	Type() NodeType
}

type KVStore interface {
	Put(key string, value Node)
	Get(key string) (Node, bool)
}

type MapKVStore map[string]Node

func (m MapKVStore) Put(key string, value Node) {
	m[key] = value
}

func (m MapKVStore) Get(key string) (Node, bool) {
	value, ok := m[key]
	return value, ok
}

func calculateHash(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

func storeFile(store KVStore, file []byte) *Object {
	data := file
	blob := Object{Data: data, Links: nil, Type: FILE}
	hash := calculateHash(data)
	store.Put(hex.EncodeToString(hash), blob)
	return &blob
}

func storeDirectory(store KVStore, dir Node) *Object {
	var links []Link
	it := dir.(Dir).It() //遍历目录节点下的所有子节点
	for it.Next() {
		n := it.Node() //当前目录下的node
		switch n.Type() {
		case FILE:
			file := n.(File)
			tmp := storeFile(store, file.Bytes())
			jsonMarshal, _ := json.Marshal(tmp)
			hash := calculateHash(jsonMarshal)
			links = append(links, Link{
				Hash: hash,
				Size: int(file.Size()),
			})
		case DIR:
			subDir := n.(Dir)
			tmp := storeDirectory(store, subDir) //递归，直到遍历到文件，此时目录入栈
			jsonMarshal, _ := json.Marshal(tmp)
			hash := calculateHash(jsonMarshal)
			links = append(links, Link{
				Hash: hash,
				Size: len(jsonMarshal),
			})
		}
	}
	dirObject := Object{Links: links, Type: DIR}
	jsonMarshal, _ := json.Marshal(dirObject)
	hash := calculateHash(jsonMarshal)
	store.Put(hex.EncodeToString(hash), dirObject)
	return &dirObject
}

func splitFile(file []byte) [][]byte {
	var chunks [][]byte
	for i := 0; i < len(file); i += 256 * 1024 {
		end := i + 256*1024
		if end > len(file) {
			end = len(file)
		}
		chunks = append(chunks, file[i:end])
	}
	return chunks
}

func dfsForStoreFile(height int, file File, store KVStore, seedId int, h *sha256.Hash) (*Object, int) {
	if height == 1 {
		if (len(file.Bytes()) - seedId) <= 256*1024 {
			data := file.Bytes()[seedId:] //截取从seedId到最后
			blob := Object{Data: data, Links: nil}
			jsonMarshal, _ := json.Marshal(blob)
			hash := calculateHash(jsonMarshal)
			store.Put(hex.EncodeToString(hash), blob)
			return &blob, len(data)
		}
		links := &Object{}
		lenData := 0
		for i := 1; i <= 4096; i++ {
			end := seedId + 256*1024
			if len(file.Bytes()) < end {
				end = len(file.Bytes())
			}
			data := file.Bytes()[seedId:end]
			blob := Object{Data: data, Links: nil}
			lenData += len(data)
			jsonMarshal, _ := json.Marshal(blob)
			hash := calculateHash(jsonMarshal)
			store.Put(hex.EncodeToString(hash), blob)
			links.Links = append(links.Links, Link{
				Hash: hash,
				Size: len(data),
			})
			links.Data = append(links.Data, []byte("blob")...)
			seedId += 256 * 1024
			if seedId >= len(file.Bytes()) {
				break
			}
		}
		jsonMarshal, _ := json.Marshal(links)
		hash := calculateHash(jsonMarshal)
		store.Put(hex.EncodeToString(hash), links)
		return links, lenData
	} else {
		links := &Object{}
		lenData := 0
		for i := 1; i <= 4096; i++ {
			if seedId >= len(file.Bytes()) {
				break
			}
			tmp, lens := dfsForStoreFile(height-1, file, store, seedId, h)
			lenData += lens
			jsonMarshal, _ := json.Marshal(tmp)
			hash := calculateHash(jsonMarshal)
			links.Links = append(links.Links, Link{
				Hash: hash,
				Size: lens,
			})
			typeName := "link"
			if tmp.Links == nil {
				typeName = "blob"
			}
			links.Data = append(links.Data, []byte(typeName)...)
		}
		jsonMarshal, _ := json.Marshal(links)
		hash := calculateHash(jsonMarshal)
		store.Put(hex.EncodeToString(hash), links)
		return links, lenData
	}
}

func Add(store KVStore, node Node) []byte {
	h := sha256.New()
	if node.Type() == FILE {
		file := node.(File)
		if len(file.Bytes()) <= 256*1024 {
			// If the file is smaller than 256KB, store it directly
			blob := storeFile(store, file.Bytes())
			hash := calculateHash(blob.Data)
			return hash
		} else {
			// If the file is larger than 256KB, split it into chunks and store each chunk
			linkLen := (len(file.Bytes()) + (256*1024 - 1)) / (256 * 1024)
			height := int(math.Ceil(math.Log2(float64(linkLen)) / math.Log2(4096)))
			tmp, _ := dfsForStoreFile(height, file, store, 0, h)
			jsonMarshal, _ := json.Marshal(tmp)
			hash := calculateHash(jsonMarshal)
			return hash
		}
	} else {
		// If the node is a directory, store it
		dir := node.(Dir)
		tmp := storeDirectory(store, dir)
		jsonMarshal, _ := json.Marshal(tmp)
		hash := calculateHash(jsonMarshal)
		return hash
	}
}
