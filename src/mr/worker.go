package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function. 向master发送rpc请求任务
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	numFiles := 0
	numReduce := 0
	//获得文件名
	for {
		re := MapCall()
		numFiles = re.NumFiles
		numReduce = re.NumReduce
		if re.NumFiles == re.NumMap {
			break
		}
		//map部分
		if re.FileName == "" {
			continue
		}
		file, err := os.Open(re.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", re.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", re.FileName)
		}
		file.Close()
		encoders := []*json.Encoder{}
		files := []*os.File{}
		//crash时，mapf出现问题了,可能会gi掉或者超时
		kva := mapf(re.FileName, string(content))
		//创建临时文件，为临时文件创建encoder
		for i := 0; i < re.NumReduce; i++ {
			ofile, _ := ioutil.TempFile("/Users/shujinsong/go/src/mit/mit6.824/src/main/mr-tmp/", "")
			files = append(files, ofile)
			enc := json.NewEncoder(ofile)
			encoders = append(encoders, enc)
		}
		//按键值对写入文件
		for _, v := range kva {
			//写入相对reduce的文件名称
			err := encoders[ihash(v.Key)%re.NumReduce].Encode(&v)
			if err != nil {
				log.Fatalf("encode file")
			}
		}
		//完成任务后，该写文件名称
		for i := 0; i < re.NumReduce; i++ {
			oname := "mr-" + fmt.Sprintf("%d", re.MapTaskId) + "-" + fmt.Sprintf("%d", i)
			os.Rename(files[i].Name(), "/Users/shujinsong/go/src/mit/mit6.824/src/main/mr-tmp/"+oname)
		}
		call("Master.MapFinishHandler", &Args{Id: re.MapTaskId}, &Reply{})
	}
	//同步一下
	//等待所有map步骤完成
	call("Master.WorkFinishHandler", &Args{}, &Reply{})
	for {
		//获得reduceid
		re := ReduceCall()
		if re.ReduceNum == numReduce {
			break
		}
		//reduce操作,根据reduceid读取对应文件,不用临时文件
		intermediate := []KeyValue{}
		for i := 0; i < numFiles; i++ {
			file, err := os.Open("/Users/shujinsong/go/src/mit/mit6.824/src/main/mr-tmp/" + "mr-" + fmt.Sprintf("%d", i) + "-" + fmt.Sprintf("%d", re.ReduceId))
			if err != nil {
				log.Fatalf("cannot open %s", "mr-"+fmt.Sprintf("%d", i)+"-"+fmt.Sprintf("%d", re.ReduceId))
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			file.Close()
			// os.Remove("/Users/shujinsong/go/src/mit/mit6.824/src/main/" + "mr-" + fmt.Sprintf("%d", i) + "-" + fmt.Sprintf("%d", re.ReduceId))
		}
		//排序
		sort.Sort(ByKey(intermediate))
		//设置reduceid
		i := 0
		oname := "mr-out-" + fmt.Sprintf("%d", re.ReduceId)
		ofile, _ := ioutil.TempFile("/Users/shujinsong/go/src/mit/mit6.824/src/main/mr-tmp/", "")
		// ofile, _ := os.Create(oname)
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			//这里可能挂
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
		os.Rename(ofile.Name(), "/Users/shujinsong/go/src/mit/mit6.824/src/main/mr-tmp/"+oname)
		call("Master.ReduceFinishHandler", &Args{Id: re.ReduceId}, &Reply{})
	}
	call("Master.TaskFinishHandler", &ReduceArgs{}, &ReduceReply{})

}
func MapCall() Reply {

	// declare an argument structure. 声明参数
	args := Args{}

	// fill in the argument(s). 填写参数

	// declare a reply structure. 声明回复
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.MapHandler", &args, &reply)
	// reply.Y should be 100.

	return reply
}
func ReduceCall() ReduceReply {
	args := ReduceArgs{}
	reply := ReduceReply{}
	call("Master.ReduceHandler", &args, &reply)
	return reply
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure. 声明参数
	args := ExampleArgs{}

	// fill in the argument(s). 填写参数
	args.X = 99

	// declare a reply structure. 声明回复
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
