package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

type putTreeResult struct {
	path      string
	succeeded bool
}

type PutTree struct {
	count          uint64
	result_counter uint64
	results        chan string
}

func NewPutTree() *PutTree {
	return &PutTree{results: make(chan string)}
}

func (puttree *PutTree) AddFile() {
	atomic.AddUint64(&puttree.count, 1)
}

func (puttree *PutTree) AddResult() {
	atomic.AddUint64(&puttree.result_counter, 1)
}

func (puttree *PutTree) ResultCount() uint64 {
	return atomic.LoadUint64(&puttree.result_counter)
}

func (puttree *PutTree) Count() uint64 {
	return atomic.LoadUint64(&puttree.count)
}

func (puttree *PutTree) PutToS3(path string, info os.FileInfo, err error) error {

	if !info.IsDir() {
		puttree.AddFile()
		go puttree.putToS3(path)
	}

	return nil
}

func (puttree *PutTree) putToS3(path string) {
	time.Sleep(time.Nanosecond)
	fmt.Printf("send %s\n", path)
	puttree.results <- path
}

func (puttree *PutTree) WaitForIt() {
Loop:
	for {
		select {
		case path := <-puttree.results:
			puttree.AddResult()
			fmt.Println(path)
			if puttree.ResultCount() == puttree.Count() {
				break Loop
			}
		}
	}
}

func main() {
	putter := NewPutTree()
	filepath.Walk("./", putter.PutToS3)
	putter.WaitForIt()
	fmt.Println(putter.Count())
}
