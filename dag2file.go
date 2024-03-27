package merkledag

// Hash to file
func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte {
	// 根据hash和path， 返回对应的文件, hash对应的类型是tree
	treeObj := getObjectByHash(store, hash)
	obj := getNode(store, path, hp, *treeObj)
	var data []byte
	if obj.Links != nil {
		data = obj.Data
	} else {
		data = getDfsData(store, *obj, data)
	}
	return data
}

func getDfsData(store KVStore, object Object, data []byte) []byte {
	for _, link := range object.Links {
		obj := getObjectByHash(store, link.Hash)
		if obj.Links != nil {
			data = getDfsData(store, *obj, data)
		} else {
			data = append(data, obj.Data...)
		}
	}
	return data
}

func getNode(store KVStore, path string, hp HashPool, object Object) Object {
	for _, part := range splitPath(path) {
		var dirHash []byte
		for _, link := range object.Links {
			if link.Name == part {
				dirHash = link.Hash
				break
			}
		}
		jsonObj, _ := store.Get(dirHash)
		var obj Object
		err := json.Unmarshal(jsonObj, &obj)
		if err != nil {
			fmt.Println("解析字符串错误")
		} else {
			object = obj
		}
	}
	return object
}

func splitPath(path string) []string {
	return strings.Split(path, "/")
}

func getObjectByHash(store KVStore, hash []byte) *Object {
	obj := &Object{}
	jsonTreeObj, _ := store.Get(hash)
	err := json.Unmarshal(jsonTreeObj, &obj)
	if err != nil {
		fmt.Println("解析字符串错误")
	}
	return obj
}
