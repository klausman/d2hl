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
	"runtime"
	"strings"
	"sync"
	"syscall"

	log "github.com/inconshreveable/log15"

	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
)

var (
	dryrun   = flag.Bool("dryrun", false, "Do not do anything, just print what would be done")
	jobs     = flag.Int("jobs", runtime.NumCPU(), "Number of parallel jobs to use when checksumming")
	pathlist []string
)

type treeinfo struct {
	RWLock    *sync.RWMutex
	Sums      map[string][]string
	Inodes    map[uint64]bool
	DupeCount int
	FileCount int
	pb        *progressbar.ProgressBar
}

func newTI() treeinfo {
	var ti treeinfo
	var newmtx sync.RWMutex
	ti.Sums = make(map[string][]string)
	ti.Inodes = make(map[uint64]bool)
	ti.RWLock = &newmtx
	return ti
}

func (ti *treeinfo) process(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	ti.pb.Add(1)
	if !info.Mode().IsRegular() {
		return nil
	}
	if strings.HasSuffix(path, ".tmpdedupe") {
		log.Crit("Leftover file from previous run, please investigate", "path", path)
		os.Exit(-1)
	}
	ti.FileCount++
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		log.Crit("We somehow got a file without an inode number", "path", path)
		os.Exit(-1)
	}

	if ok = ti.Inodes[stat.Ino]; ok {
		log.Debug("We have lready seen this i-node", "inodenum", stat.Ino)
		return nil
	}
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

func (ti *treeinfo) checksum(id int, p chan string, wg *sync.WaitGroup) {
	wlog := log.New("workerid", id)
	wlog.Debug("Worker starting")
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
		wlog.Debug("Checksum", "path", path, "sum", s)
		ti.RWLock.Lock()
		ti.Sums[s] = append(ti.Sums[s], path)
		ti.RWLock.Unlock()
		ti.pb.Add(1)
	}
	wlog.Debug("Worker exiting")
}

func dedupe(ti *treeinfo) int64 {
	var savings int64
	ti.pb = progressbar.Default(int64(len(pathlist)), "Cmp/Link")
	for _, names := range ti.Sums {
		ti.pb.Add(1)
		if len(names) <= 1 {
			continue
		}
		first := names[0]
		fi, err := os.Stat(first)
		check(err)
		size := fi.Size()
		for _, name := range names[1:] {
			if *dryrun {
				log.Warn("Would deduplicate", "src", name, "dest", first)
			} else {
				log.Info("Deduping", "src", name, "dest", first)
				tmpname := fmt.Sprintf("%s.tmpdedupe", name)
				err := os.Rename(name, tmpname)
				check(err)
				err = os.Link(first, name)
				check(err)
				err = os.Remove(tmpname)
				check(err)
			}
			savings += size
			ti.DupeCount++
		}
	}
	return savings
}

func main() {
	flag.Parse()
	logSetup("", false)

	var root string
	ti := newTI()
	args := flag.Args()
	if len(args) == 0 {
		root = "."
	} else {
		root = args[0]
	}
	ti.pb = progressbar.NewOptions(-1, progressbar.OptionSpinnerType(9))
	ti.pb.Describe("Finding files")
	err := filepath.Walk(root, ti.process)
	check(err)
	ti.pb.Finish()
	log.Info("Files enumerated", "total", ti.FileCount, "tocheck", len(pathlist))

	ti.pb = progressbar.Default(int64(len(pathlist)), "Checksum")

	c := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < *jobs; i++ {
		go ti.checksum(i, c, &wg)
		wg.Add(1)
	}
	for _, path := range pathlist {
		c <- path
	}
	close(c)
	wg.Wait()
	s := dedupe(&ti)
	log.Info("Deduplication complete", "freedspace", humanize.Bytes(uint64(s)))
}
