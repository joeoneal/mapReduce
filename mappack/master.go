package mappack

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type ShutdownArgs struct{}
type ShutdownReply struct{}

type TaskRequest struct {
	WorkerAddress string
}

type TaskReply struct {
	Type       string
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type MasterRPC struct {
	tempdir     string
	myAddress   string
	client      Interface
	numMap      int
	numReduce   int
	mapTasks    []*MapTask
	reduceTasks []*ReduceTask

	mu        sync.Mutex
	stage     string
	taskIndex int
}

type StartConfig struct {
	IsMaster       bool
	IsWorker       bool
	InputFile      string
	MasterAddr     string
	Port           int
	NumMapTasks    int
	NumReduceTasks int
}

func Start(client Interface, cfg StartConfig) error {
	switch {
	case cfg.IsMaster:
		log.Println("starting in master mode.")
		return runMaster(client, cfg)
	case cfg.IsWorker:
		log.Println("starting in worker mode.")
		return runWorker(client, cfg)
	default:
		return errors.New("must register as --master or --worker")
	}
}

func (m *MasterRPC) RequestTask(req TaskRequest, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("[master] received task request from worker %s during stage: %s", req.WorkerAddress, m.stage)

	switch m.stage {
	case "map":
		if m.taskIndex < len(m.mapTasks) {
			reply.Type = "map"
			reply.MapTask = m.mapTasks[m.taskIndex]
			log.Printf("[master] assigning map task %d to worker %s", m.taskIndex, req.WorkerAddress)
			m.taskIndex++
		} else {
			m.prepareReduceTasks()
			reply.Type = "none"
			log.Printf("[master] no more map tasks. Reduce task prep initiated.")
		}

	case "reduce":
		if m.taskIndex < len(m.reduceTasks) {
			reply.Type = "reduce"
			reply.ReduceTask = m.reduceTasks[m.taskIndex]
			log.Printf("[master] assigning reduce task %d to worker %s", m.taskIndex, req.WorkerAddress)
			m.taskIndex++
		} else {
			m.stage = "done"
			reply.Type = "none"
			log.Println("[master] all reduce tasks assigned. Master entering done state.")
		}

	case "done":
		reply.Type = "none"
		log.Printf("[master] no tasks to assign. Master is in done state.")
	}

	return nil
}

func (m *MasterRPC) prepareReduceTasks() {
	m.stage = "reduce"
	m.taskIndex = 0
	log.Println("[master] preparing reduce tasks.")

	for i := 0; i < m.numReduce; i++ {
		task := &ReduceTask{
			M:           m.numMap,
			R:           m.numReduce,
			N:           i,
			SourceHosts: make([]string, m.numMap),
		}
		for j := range task.SourceHosts {
			task.SourceHosts[j] = m.myAddress
		}
		m.reduceTasks = append(m.reduceTasks, task)
		log.Printf("[master] prepared reduce task %d", i)
	}

	log.Println("[master] all map tasks assigned. Transitioning to reduce phase.")
}

func runMaster(client Interface, cfg StartConfig) error {
	tempdir := filepath.Join(os.TempDir(), "mapreduce.3410")
	if err := os.RemoveAll(tempdir); err != nil {
		return fmt.Errorf("could not clear tempdir: %v", err)
	}
	if err := os.Mkdir(tempdir, 0700); err != nil {
		return fmt.Errorf("could not create tempdir: %v", err)
	}

	log.Printf("[master] created tempdir at %s", tempdir)

	if err := prepareMapInput(cfg.InputFile, cfg.NumMapTasks, tempdir); err != nil {
		return err
	}

	httpAddr := fmt.Sprintf("localhost:%d", cfg.Port)
	rpcAddr := fmt.Sprintf("localhost:%d", cfg.Port+1)

	startHTTPServer(httpAddr, tempdir)
	time.Sleep(2 * time.Second)

	master := &MasterRPC{
		tempdir:   tempdir,
		myAddress: httpAddr,
		client:    client,
		numMap:    cfg.NumMapTasks,
		numReduce: cfg.NumReduceTasks,
		stage:     "map",
	}

	for i := 0; i < cfg.NumMapTasks; i++ {
		master.mapTasks = append(master.mapTasks, &MapTask{
			M:          cfg.NumMapTasks,
			R:          cfg.NumReduceTasks,
			N:          i,
			SourceHost: httpAddr,
		})
		log.Printf("[master] created map task %d", i)
	}

	if err := rpc.Register(master); err != nil {
		return fmt.Errorf("rpc register failed: %v", err)
	}

	rpcListener, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return fmt.Errorf("failed to start RPC listener: %v", err)
	}
	go rpc.Accept(rpcListener)

	log.Printf("master ready awaiting workers")

	for master.stage != "done" {
		time.Sleep(1 * time.Second)
	}

	return finalizeOutput(master.tempdir, cfg.NumReduceTasks)
}

func prepareMapInput(inputFile string, numParts int, tempdir string) error {
	log.Printf("[master] splitting input %s into %d parts", inputFile, numParts)
	var paths []string
	for i := 0; i < numParts; i++ {
		paths = append(paths, filepath.Join(tempdir, mapSourceFile(i)))
	}
	if err := splitDatabase(inputFile, paths); err != nil {
		return fmt.Errorf("split failed: %v", err)
	}
	for i := 0; i < numParts; i++ {
		src := filepath.Join(tempdir, mapSourceFile(i))
		dst := filepath.Join(tempdir, "ready_"+mapSourceFile(i))
		copyFile(src, dst)
		log.Printf("[master] prepared map input file %s", dst)
	}
	return nil
}

func startHTTPServer(addr, dir string) {
	go func() {
		log.Printf("[master] starting http server at %s to serve %s", addr, dir)
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(dir))))
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("[master] http server error: %v", err)
		}
	}()
}

func finalizeOutput(tempdir string, numReduce int) error {
	var paths []string
	for i := 0; i < numReduce; i++ {
		paths = append(paths, filepath.Join(tempdir, reduceOutputFile(i)))
	}
	finalPath := filepath.Join(".", "final_output.db")
	log.Println("[master] merging reduce outputs into final database.")
	if err := mergeFinalOutput(paths, finalPath); err != nil {
		log.Printf("[master] failed to merge final output: %v", err)
		return err
	}
	log.Printf("[master] final output written to %s", finalPath)
	log.Println("[master] Master completed all tasks.")
	return nil
}

func copyFile(src, dst string) {
	in, err := os.Open(src)
	if err != nil {
		log.Fatalf("[master] error opening source file %s: %v", src, err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		log.Fatalf("[master] error creating destination file %s: %v", dst, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		log.Fatalf("[master] error copying from %s to %s: %v", src, dst, err)
	}
	log.Printf("[master] copied %s to %s", src, dst)
}

func mergeFinalOutput(paths []string, outPath string) error {
	if err := os.RemoveAll(outPath); err != nil {
		return fmt.Errorf("mergeFinalOutput: could not remove existing file: %v", err)
	}

	finalDB, err := createDatabase(outPath)
	if err != nil {
		return fmt.Errorf("mergeFinalOutput: creating output db: %v", err)
	}
	defer finalDB.Close()

	insertStmt, err := finalDB.Prepare("INSERT INTO pairs (key, value) VALUES (?, ?)")
	if err != nil {
		return fmt.Errorf("mergeFinalOutput: preparing insert statement: %v", err)
	}
	defer insertStmt.Close()

	for _, path := range paths {
		log.Printf("[master] merging %s into final output", path)
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			return fmt.Errorf("mergeFinalOutput: opening input db %s: %v", path, err)
		}

		rows, err := db.Query("SELECT key, value FROM pairs")
		if err != nil {
			db.Close()
			return fmt.Errorf("mergeFinalOutput: reading from %s: %v", path, err)
		}

		for rows.Next() {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				rows.Close()
				db.Close()
				return fmt.Errorf("mergeFinalOutput: scanning row in %s: %v", path, err)
			}
			if _, err := insertStmt.Exec(key, value); err != nil {
				rows.Close()
				db.Close()
				return fmt.Errorf("mergeFinalOutput: inserting into final db: %v", err)
			}
		}
		rows.Close()
		db.Close()
	}
	log.Printf("[master] final output successfully merged to %s", outPath)
	return nil
}
