package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// global lock for worker access task
var mu sync.Mutex

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     State     // 任务的状态
	StartTime time.Time // 任务的开始时间，为crash做准备
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	TaskChannelReduce chan *Task     // 使用chan保证并发安全
	TaskChannelMap    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
//Done 主函数mr调用，如果所有task完成mr会通过此方法退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}

}

// 自增 task id
func (c *Coordinator) genTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

// push taskInfo if not in TaskMetaHolder
func (t *TaskMetaHolder) acceptMeta(taskInfo *TaskMetaInfo) bool {
	taskId := taskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = taskInfo
	}
	return true
}

// make a map task for each file
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		task := Task{TaskType: MapTask, TaskId: c.genTaskId(), ReducerNum: c.ReducerNum, FileSlice: []string{file}}

		taskMetaInfo := TaskMetaInfo{state: Waiting, TaskAdr: &task}

		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("make a map task :", &task)
		c.TaskChannelMap <- &task
	}
	fmt.Println("1")
}

// make a reduce task for each file
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.genTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
		}
		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		//fmt.Println("make a reduce task :", &task)
		c.TaskChannelReduce <- &task
	}
}

func selectReduceName(i int) []string {
	var res []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tmp") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
			res = append(res, file.Name())
		}
	}
	return res
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) isNotWorking(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

// 任务是否完成并且可以进入下一阶段
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	for _, task := range t.MetaMap {
		if task.TaskAdr.TaskType == MapTask {
			if task.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if task.TaskAdr.TaskType == ReduceTask {
			if task.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	// map -> reduce
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
		// reduce -> done
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {

		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {

		c.DistPhase = AllDone

	}
}

// assign task
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch c.DistPhase {
	case MapPhase:
		{
			// 如果存在任务就分发任务
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap

				if !c.taskMetaHolder.isNotWorking(reply.TaskId) {
					fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
					break
				}
			} else {
				reply.TaskType = WaittingTask
				// 检查任务是否完成，如果完成进入下一个阶段
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce

				if !c.taskMetaHolder.isNotWorking(reply.TaskId) {
					fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
					break
				}
			} else {
				reply.TaskType = WaittingTask
				// 检查任务是否完成，如果完成进入下一个阶段
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		{
			panic("The phase undefined ! ! !")
		}
	}

	return nil
}

// worker 向 Coordinator表明任务已经完成
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		{
			meta, fDone := c.taskMetaHolder.MetaMap[args.TaskId]

			if fDone && meta.state == Working {
				meta.state = Done
				fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
			} else {
				fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
			}
			break
		}
	case ReduceTask:
		{
			meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

			//prevent a duplicated work which returned from another worker
			if ok && meta.state == Working {
				meta.state = Done
				fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
			} else {
				fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
			}
			break
		}
	default:
		panic("The task type undefined ! ! !")
	}
	return nil
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working {
				//fmt.Println("task[", v.TaskAdr.TaskId, "] is working: ", time.Since(v.StartTime), "s")
			}

			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))

				switch v.TaskAdr.TaskType {
				case MapTask:
					c.TaskChannelMap <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.TaskChannelReduce <- v.TaskAdr
					v.state = Waiting

				}
			}
		}
		mu.Unlock()
	}

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
		},
	}
	c.makeMapTasks(files)

	c.server()

	go c.CrashDetector()
	return &c
}
