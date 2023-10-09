package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	lock            sync.Mutex
	cond            *sync.Cond
	reduceId        int
	reduceNum       int
	numReduce       int
	numMap          int
	numWorkerFinish int
	numWorker       int
	taskFinish      bool
	fileName        []string
	fileFinished    []bool
	filePubliced    []bool
	reduceFinished  []bool
	reducePubliced  []bool
}

// Your code here -- RPC handlers for the worker to call.
// 分发map任务，感觉可以等😂
func (m *Master) MapHandler(args *Args, reply *Reply) error {
	reply.NumFiles = len(m.fileName)
	//增加map任务编号，这感觉没必要
	reply.NumMap = m.numMap
	//总共reduce任务数目
	reply.NumReduce = m.numReduce
	//寻找未完成的文件名称,观察已发布mapfile情况
	for i := 0; i < len(m.fileName); i++ {
		if !m.filePubliced[i] {
			reply.FileName = m.fileName[i]
			reply.MapTaskId = i
			m.lock.Lock() //不立刻设置为true，10s内收到信号则设置为true，但该任务10s内不能发布
			m.filePubliced[i] = true
			m.lock.Unlock()
			return nil
		}
	}
	return nil
}

// map任务完成后的事情,将该文件设置为完成
func (m *Master) MapFinishHandler(args *Args, reply *Reply) error {
	if !m.fileFinished[args.Id] {
		m.lock.Lock()
		m.fileFinished[args.Id] = true
		m.numMap++
		m.lock.Unlock()
	}
	return nil
}

// 解放所有完成map操作的woker
func (m *Master) WorkFinishHandler(args *Args, reply *Reply) error {
	for i := 0; i < len(m.fileName); i++ {
		//有一个未完成就wait
		if !m.fileFinished[i] {
			m.cond.Wait()
			return nil
		}
	}
	m.cond.Broadcast()
	return nil
}

// 每个woker进行reduce操作
func (m *Master) ReduceHandler(args *ReduceArgs, reply *ReduceReply) error {
	reply.ReduceNum = m.reduceNum //传递已经完成的个数
	for i := 0; i < m.numReduce; i++ {
		if !m.reducePubliced[i] {
			reply.ReduceId = i //传递reduce id任务id
			m.lock.Lock()      //10s内未收到信号则设置为false，但该任务10s内不能发布
			m.reducePubliced[i] = true
			m.lock.Unlock()
			return nil
		}
	}
	return nil
}

// reduce任务完成后的事情,将该文件设置为完成
func (m *Master) ReduceFinishHandler(args *Args, reply *Reply) error {
	if !m.reduceFinished[args.Id] {
		m.lock.Lock()
		m.reduceFinished[args.Id] = true
		m.reduceNum++ //已经完成数目
		m.lock.Unlock()
	}
	return nil
}

func (m *Master) TaskFinishHandler(args *ReduceArgs, reply *ReduceReply) error {
	for i := 0; i < m.numReduce; i++ {
		if !m.reduceFinished[i] {
			m.cond.Wait()
			return nil
		}
	}
	m.cond.Broadcast()
	m.taskFinish = true
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	// Your code here.
	return m.taskFinish
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		fileName:       files,
		numReduce:      nReduce,
		fileFinished:   make([]bool, len(files)),
		filePubliced:   make([]bool, len(files)),
		reduceFinished: make([]bool, nReduce),
		reducePubliced: make([]bool, nReduce),
		numMap:         0,
		lock:           sync.Mutex{},
		cond:           sync.NewCond(&sync.Mutex{}),
	}
	// Your code here.
	//启动协程检查,是否有发布的任务没有完成

	go func() {
		for {
			for i := 0; i < len(files); i++ {
				if m.filePubliced[i] && !m.fileFinished[i] {
					go func(index int) {
						time.Sleep(10 * time.Second)
						if m.filePubliced[index] && !m.fileFinished[index] {
							m.filePubliced[index] = false
						}
					}(i)

				}
			}
		}

	}()
	go func() {
		for {
			for i := 0; i < nReduce; i++ {
				if m.reducePubliced[i] && !m.reduceFinished[i] {
					go func(index int) {
						time.Sleep(10 * time.Second)
						if m.reducePubliced[index] && !m.reduceFinished[index] {
							m.reducePubliced[index] = false
						}
					}(i)

				}
			}
		}

	}()
	//生成server监听来自worker请求
	m.server()
	return &m
}
