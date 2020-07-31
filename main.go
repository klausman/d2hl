// Copyright 2020 Tobias Klausmann
// License: Apache 2.0, see LICENSE for details
package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
)

var (
	verbose  = flag.Bool("verbose", false, "Be verbose about what's going on")
	quiet    = flag.Bool("quiet", false, "Do not output summary of actions")
	dryrun   = flag.Bool("dryrun", false, "Do not do anything, just print what would be done")
	pathlist []string
)

type treeinfo struct {
	RWLock    sync.RWMutex
	Sums      map[string][]string
	Inodes    map[uint64]bool
	DupeCount int
	FileCount int
}

func NewTI() treeinfo {
	var ti treeinfo
	ti.Sums = make(map[string][]string)
	ti.Inodes = make(map[uint64]bool)
	return ti
}

func (ti *treeinfo) process(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	if strings.HasSuffix(path, ".tmpdedupe") {
		panic(fmt.Sprintf(
			"'%s' indicates previous failed run, please investigate", path))
	}
	ti.FileCount += 1
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		panic(fmt.Sprintf("Somehow got a file without Inode# for '%s'", path))
	}

	if ok = ti.Inodes[stat.Ino]; ok {
		//fmt.Fprintf(os.Stderr, "Already seen i-node %d\n", stat.Ino)
		return nil
	}
	//fmt.Printf("%s: %d\n", path, stat.Ino)
	ti.Inodes[stat.Ino] = true
	pathlist = append(pathlist, path)
	return nil
}

func (ti treeinfo) String() string {
	var r []string
	for sum, paths := range ti.Sums {
		r = append(r, fmt.Sprintf("%s: %s", sum, strings.Join(paths, " ")))
	}
	return strings.Join(r, "\n")
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func (ti *treeinfo) checksum(p chan string, wg *sync.WaitGroup) {
	//fmt.Fprintf(os.Stderr, "Goroutine starting\n")
	defer wg.Done()
	for path := range p {
		f, err := os.Open(path)
		if err != nil {
			continue
		}

		h := sha1.New()
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			continue
		}
		f.Close()
		s := fmt.Sprintf("%x", h.Sum(nil))
		if *verbose {
			fmt.Fprintf(os.Stderr, "%s %s\n", s, path)
		}
		ti.RWLock.Lock()
		ti.Sums[s] = append(ti.Sums[s], path)
		ti.RWLock.Unlock()
	}
	//fmt.Fprintf(os.Stderr, "Goroutine exiting\n")
}

func dedupe(ti *treeinfo) int64 {
	var savings int64
	for _, names := range ti.Sums {
		if len(names) <= 1 {
			continue
		}
		first := names[0]
		fi, err := os.Stat(first)
		check(err)
		size := fi.Size()
		for _, name := range names[1:] {
			if *dryrun {
				fmt.Printf("Would dedupe %s with %s\n", name, first)
			} else {
				if *verbose {
					fmt.Fprintf(os.Stderr, "Deduping %s to %s\n", name, first)
				}
				tmpname := fmt.Sprintf("%s.tmpdedupe", name)
				err := os.Rename(name, tmpname)
				check(err)
				err = os.Link(first, name)
				check(err)
				err = os.Remove(tmpname)
				check(err)
			}
			savings += size
			ti.DupeCount += 1
		}
	}
	return savings
}

func main() {
	flag.Parse()
	if *verbose && *quiet {
		fmt.Fprintf(os.Stderr, "--quiet and --verbose are mutually exclusive\n")
		os.Exit(-1)
	}
	var root string
	ti := NewTI()
	args := flag.Args()
	if len(args) == 0 {
		root = "."
	} else {
		root = args[0]
	}
	err := filepath.Walk(root, ti.process)
	check(err)
	if *verbose {
		fmt.Fprintf(os.Stderr, "Found %d files, %d to checksum\n", ti.FileCount, len(pathlist))
	}

	c := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		go ti.checksum(c, &wg)
		wg.Add(1)
	}
	start := time.Now()
	for _, path := range pathlist {
		//fmt.Fprintf(os.Stderr, "send: %s\n", path)
		c <- path
	}
	close(c)
	wg.Wait()
	if !*quiet {
		t := time.Since(start)
		fmt.Fprintf(os.Stderr, "Checksummed %d files in %s, %.0f f/s\n", len(pathlist), t, float64(len(pathlist))/t.Seconds())
	}
	start = time.Now()
	s := dedupe(&ti)
	if !*quiet {
		t := time.Since(start)
		fmt.Fprintf(os.Stderr, "Deduped %d files in %s, %.0f f/s\n", ti.DupeCount, t, float64(ti.DupeCount)/t.Seconds())
		fmt.Printf("Saved %s of disk space\n", humanize.Bytes(uint64(s)))
	}
}
