package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type KeyGroup struct {
	key    string
	values <-chan string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////
func groupby(rawPairs <-chan Pair) <-chan KeyGroup {
	// Create the output channel for KeyGroups
	groups := make(chan KeyGroup)

	// Start a goroutine to process the incoming pairs
	go func() {
		// Ensure the groups channel gets closed when we're done
		defer close(groups)

		var currentKey string
		var valuesChan chan string

		for pair := range rawPairs {
			// If this is a new key or the first key
			if pair.Key != currentKey || valuesChan == nil {
				// Close the previous values channel if it exists
				if valuesChan != nil {
					close(valuesChan)
				}

				// Create a new values channel for this key
				valuesChan = make(chan string)

				// Update the current key
				currentKey = pair.Key

				// Send a new KeyGroup with this key and values channel
				groups <- KeyGroup{
					key:    currentKey,
					values: valuesChan,
				}
			}

			// Send the value to the current values channel
			valuesChan <- pair.Value
		}

		// Close the final values channel when rawPairs is closed
		if valuesChan != nil {
			close(valuesChan)
		}
	}()

	return groups
}

////////////////////////////////////////////////////////////////////////////////////////////////////////

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	localaddress := localAddr.IP.String()

	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return localaddress
}

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}

//////////////////////////////////////////////////////////////////////////

func (task *MapTask) Process(tempdir string, client Interface) error {
	inputFile := filepath.Join(tempdir, mapInputFile(task.N))
	inPath := filepath.Join(tempdir, mapInputFile(task.N))

	if err := download(makeURL(task.SourceHost, mapSourceFile(task.N)), inputFile); err != nil {
		log.Printf("Error downloading input file %v\n", err)
		return err
	}

	db, err := openDatabase(inPath)
	if err != nil {
		log.Printf("Error opening input database %v\n", err)
		return err
	}
	defer db.Close()

	outputDatabases := make([]*sql.DB, task.R)
	for i := 0; i < task.R; i++ {
		outPath := filepath.Join(tempdir, mapOutputFile(task.N, i))
		outputDB, err := createDatabase(outPath)
		if err != nil {
			log.Printf("Failed to open output database %s: %v", outPath, err)
			return err
		}
		log.Printf("Writing map output file to: %s", outPath)

		outputDatabases[i] = outputDB
		defer outputDB.Close()
	}

	// read key/val pairs from input
	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		log.Printf("Error querying database: %v\n", err)
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			log.Printf("Error scanning row: %v\n", err)
		}

		dataChan := make(chan Pair, 100)
		isDone := make(chan bool, 100)

		// background go routine that reads the valude from teh channel and writes them to the database
		go func() {
			for pair := range dataChan {
				// given code to determine which output file the pair should go to
				hash := fnv.New32()
				hash.Write([]byte(pair.Key))
				r := int(hash.Sum32() % uint32(task.R))
				_, err := outputDatabases[r].Exec("INSERT INTO pairs (key, value) VALUES (?, ?)", pair.Key, pair.Value)
				if err != nil {
					log.Printf("Error inserting into output database %v\n", err)
				}
			}
			isDone <- true
		}()

		// sends pairs to channel
		go func(key, value string) {
			if err := client.Map(key, value, dataChan); err != nil {
				log.Printf("Error in Map function: %v\n", err)
			}

		}(key, value)
		<-isDone

	}
	log.Printf("Done with task %v", task.N)
	return nil

}

// ////////////////////////////////////////////////////////////////////////
func (task *ReduceTask) Process(tempdir string, client Interface) error {
	log.Printf("Reduce task %v starting now", task.N)

	inPath := filepath.Join(tempdir, reduceInputFile(task.N))
	outPath := filepath.Join(tempdir, reduceOutputFile(task.N))
	tempPath := filepath.Join(tempdir, reduceTempFile(task.N))

	log.Printf("preparing to merge databases")

	// url list pointing to map task outputs
	var inputURLs []string
	for i := 0; i < task.M; i++ {
		url := makeURL(task.SourceHosts[i], mapOutputFile(i, task.N))
		log.Printf("ReduceTask %v source host %d: %s", task.N, i, url)
		inputURLs = append(inputURLs, url)
	}

	// Merge the input databases into the temp directory
	if _, err := mergeDatabases(inputURLs, inPath, tempPath); err != nil {
		log.Printf("Could not merge: %v", err)
		return err
	}

	log.Printf("Reduce task %v merged database into %s", task.N, inPath)

	// Create output database
	log.Printf("ReduceTask %d Creating output database: %s", task.N, outPath)
	if _, err := createDatabase(outPath); err != nil {
		log.Printf("Error creating output database: %v", err)
		return err
	}

	outputDatabase, err := sql.Open("sqlite3", outPath)
	if err != nil {
		log.Printf("Error opening output database: %v", err)
		return err
	}
	defer outputDatabase.Close()

	// Open the input database
	inputDB, err := openDatabase(inPath)
	if err != nil {
		log.Printf("Error opening input database: %v", err)
		return err
	}
	defer inputDB.Close()

	log.Printf("ReduceTask %d: Querying input key-value pairs", task.N)
	rows, err := inputDB.Query("SELECT key, value FROM pairs ORDER BY key")
	if err != nil {
		log.Printf("Error querying input database: %v", err)
		return err
	}
	defer rows.Close()

	// Feed key-value pairs into the groupby input channel
	rawPairs := make(chan Pair, 100)
	done := make(chan bool)

	// reader sends pairs to channel
	go func() {
		defer close(rawPairs)
		for rows.Next() {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}
			rawPairs <- Pair{Key: key, Value: value}
		}
		log.Printf("ReduceTask %d: Finished reading input pairs", task.N)

	}()

	log.Printf("ReduceTask %d: Starting reduce phase", task.N)
	log.Printf("ReduceTask %d: Waiting to receive grouped keys", task.N)

	// Process each key group
	for group := range groupby(rawPairs) {
		// log.Printf("ReduceTask %d: Reducing key group: %s", task.N, group.key)
		outputChan := make(chan Pair, 100)

		// Writer goroutine
		go func(key string, out <-chan Pair) {
			for pair := range out {
				_, err := outputDatabase.Exec(`INSERT INTO pairs (key, value) VALUES (?, ?)`, pair.Key, pair.Value)
				if err != nil {
					log.Printf("insert error for key=%s: %v", pair.Key, err)
				}
			}
			done <- true
		}(group.key, outputChan)

		// Call Reduce
		err := client.Reduce(group.key, group.values, outputChan)
		if err != nil {
			log.Printf("Reduce error on key=%s: %v", group.key, err)
		}

		// Wait for writer
		<-done
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %v", err)
	}

	log.Printf("ReduceTask %d: Completed reduce phase, processed groups", task.N)
	log.Printf("Completed reduce task %v", task.N)
	return nil
}

//////////////////////////////////////////////////////////////////////////

func main() {
	m := 10
	r := 5
	source := "source.db"
	target := "target.db"
	tmp := os.TempDir()

	tempdir := filepath.Join(tmp, fmt.Sprintf("mapreduce.%d", os.Getpid()))
	log.Printf("temp directory is: %s", tempdir)

	if err := os.RemoveAll(tempdir); err != nil {
		log.Fatalf("unable to delete old temp dir: %v", err)
	}
	if err := os.Mkdir(tempdir, 0700); err != nil {
		log.Fatalf("unable to create temp dir: %v", err)
	}

	defer os.RemoveAll(tempdir)

	log.Printf("splitting %s into %d pieces", source, m)
	var paths []string
	for i := 0; i < m; i++ {
		paths = append(paths, filepath.Join(tempdir, mapSourceFile(i)))
	}
	if err := splitDatabase(source, paths); err != nil {
		log.Fatalf("splitting database: %v", err)
	}

	myAddress := net.JoinHostPort(getLocalAddress(), "3410")
	log.Printf("starting http server at %s", myAddress)
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))

	// bind on the port before launching the background goroutine on Serve
	// to prevent race condition with call to download below
	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		log.Fatalf("Listen error on address %s: %v", myAddress, err)
	}
	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Fatalf("Serve error: %v", err)
		}
	}()

	// build the map tasks
	var mapTasks []*MapTask
	for i := 0; i < m; i++ {
		task := &MapTask{
			M:          m,
			R:          r,
			N:          i,
			SourceHost: myAddress,
		}
		mapTasks = append(mapTasks, task)
	}

	// build the reduce tasks
	var reduceTasks []*ReduceTask
	for i := 0; i < r; i++ {
		task := &ReduceTask{
			M:           m,
			R:           r,
			N:           i,
			SourceHosts: make([]string, m),
		}
		reduceTasks = append(reduceTasks, task)
	}

	var client Client

	// process the map tasks
	for i, task := range mapTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing map task %d: %v", i, err)
		}
		for _, reduce := range reduceTasks {
			reduce.SourceHosts[i] = myAddress
		}
	}

	// process the reduce tasks
	for i, task := range reduceTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing reduce task %d: %v", i, err)
		}
	}

	// gather outputs into final target.db file
	log.Printf("merging reduce outputs into %s", target)
	var reduceURLs []string
	for i := 0; i < r; i++ {
		reduceURLs = append(reduceURLs, makeURL(myAddress, reduceOutputFile(i)))
	}
	_, err = mergeDatabases(reduceURLs, target, filepath.Join(tempdir, "final_merge.tmp"))
	if err != nil {
		log.Fatalf("merging final output: %v", err)
	}

	log.Printf("done")

}
