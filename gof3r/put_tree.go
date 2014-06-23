package main

import (
	"fmt"
	"github.com/rlmcpherson/s3gof3r"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
)

type putTreeResult struct {
	path      string
	succeeded bool
}

var puttree PutTree

type PutTree struct {
	count          uint64
	result_counter uint64
	results        chan string
	bucket         s3gof3r.Bucket
	conf           *s3gof3r.Config
	Path           string `short:"p" long:"path" description:"Path to directory."`
	Prefix         string `long:"prefix" description:"Prefix for s3."`
	CommonOpts
	Header http.Header `long:"header" short:"m" description:"HTTP headers"`
}

// func NewPutTree() *PutTree {
// 	return &PutTree{results: make(chan string)}
// }

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
	if err != nil {
		fmt.Println("WAT")
		fmt.Println(path)
		return nil
	}

	if !info.IsDir() {
		puttree.AddFile()
		go puttree.putToS3(path)
	}

	return nil
}

func (puttree *PutTree) putToS3(path string) {
	r, err := os.Open(path)
	if err != nil || path == "" {
		return
	}

	key := url.QueryEscape(filepath.Join(puttree.Prefix, path))
	fmt.Printf("%s -> %s\n", path, key)

	defer r.Close()

	bucket := puttree.bucket
	w, err := bucket.PutWriter(key, puttree.Header, puttree.conf)
	if err != nil {
		puttree.results <- ""
		return
	}

	if _, err = io.Copy(w, r); err != nil {
		puttree.results <- ""
		return
	}

	if err = w.Close(); err != nil {
		puttree.results <- ""
		return
	}

	if puttree.Debug {
		debug()
	}

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

func (puttree *PutTree) Execute(args []string) (err error) {
	conf := new(s3gof3r.Config)
	*conf = *s3gof3r.DefaultConfig
	puttree.conf = conf

	k, err := getAWSKeys()
	if err != nil {
		return
	}

	s3 := s3gof3r.New(puttree.EndPoint, k)
	puttree.bucket = *s3.Bucket(puttree.Bucket)

	s3gof3r.SetLogger(os.Stderr, "", log.LstdFlags, put.Debug)

	if puttree.Header == nil {
		puttree.Header = make(http.Header)
	}

	if puttree.Concurrency > 0 {
		conf.Concurrency = puttree.Concurrency
	}

	if puttree.WithoutSSL {
		conf.Scheme = "http"
	}

	conf.PartSize = puttree.PartSize
	conf.Md5Check = !puttree.CheckDisable

	puttree.results = make(chan string)

	fmt.Println(puttree.Path)
	filepath.Walk(puttree.Path, puttree.PutToS3)

	puttree.WaitForIt()
	return
}

func init() {
	_, err := parser.AddCommand("put-tree", "put (upload) a directory to S3", "put (upload) a directory to S3", &puttree)
	if err != nil {
		log.Fatal(err)
	}
}
