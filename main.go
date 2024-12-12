// Copyright 2020 Tobias Klausmann
// License: Apache 2.0, see LICENSE for details
package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/crypto/blake2b"
)

var (
	dryrun     = flag.Bool("dryrun", false, "Do not do anything, just print what would be done")
	jobs       = flag.Int("jobs", runtime.NumCPU(), "Number of parallel jobs to use when checksumming")
	nodotfiles = flag.Bool("nodot", false, "Exclude files starting with a dot")
	minsize    = flag.Uint64("minsize", 0, "Minimum file size to consider")
	loglevel   = flag.String("level", "info", "Log level, one of debug, info, warn, error")
	pathlist   []string
)

func main() {
	flag.Parse()
	ll, err := strToLoglevel(*loglevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(-1)
	}
	logger := logSetup(os.Stderr, ll, "20060102-15:04:05.000", true)

	var root string
	args := flag.Args()
	if len(args) == 0 {
		root = "."
	} else {
		root = args[0]
	}
	os.Exit(doD2hl(root, logger))
}

func strToLoglevel(s string) (slog.Level, error) {
	var l slog.Level
	switch s {
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "debug":
		return slog.LevelDebug, nil
	case "error":
		return slog.LevelError, nil
	}
	return l, fmt.Errorf("unknown log level '%s'", s)
}

func doD2hl(root string, logger *slog.Logger) int {
	ti := newTI()
	ti.log = logger
	logger.Info("Enumerating files", "root", root)
	start := time.Now()
	err := filepath.Walk(root, ti.process)
	if err != nil {
		logger.Error("Walking tree failed", "error", err)
		return -1
	}
	elapsed := time.Since(start)
	logger.Info("Files enumerated", "total", ti.FileCount, "tocheck", len(pathlist),
		"time", elapsed, "per_sec", float64(ti.FileCount)/elapsed.Seconds())

	//nolint:staticcheck // We do not use contexts at all
	if logger.Enabled(nil, slog.LevelInfo) {
		ti.progbar = progressbar.Default(int64(len(pathlist)), "Checksum")
	}

	start = time.Now()
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
	elapsed = time.Since(start)
	logger.Info("Files checksummed", "total", len(pathlist), "time", elapsed,
		"per_sec", float64(len(pathlist))/elapsed.Seconds())
	start = time.Now()
	s := dedupe(&ti)
	elapsed = time.Since(start)
	logger.Info("Deduplication complete", "freedspace", humanize.Bytes(s),
		"dedupes", ti.DupeCount, "time", elapsed, "per_sec", float64(ti.DupeCount)/elapsed.Seconds())
	return 0
}

type treeinfo struct {
	RWLock    *sync.RWMutex
	Sums      map[string][]string
	Inodes    map[uint64]bool
	DupeCount int
	FileCount int
	progbar   *progressbar.ProgressBar
	log       *slog.Logger
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
	if !info.Mode().IsRegular() {
		return nil
	}
	if *nodotfiles && strings.HasPrefix(info.Name(), ".") {
		return nil
	}
	sz := info.Size()
	if sz < 0 {
		ti.log.Error("Found file with negative size, please investigate", "path", path, "size", info.Size())
		os.Exit(-1)
	}
	if uint64(sz) < *minsize {
		return nil
	}
	if strings.HasSuffix(path, ".tmpdedupe") {
		ti.log.Error("Leftover file from previous run, please investigate", "path", path)
		os.Exit(-1)
	}
	ti.FileCount++
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		ti.log.Error("We somehow got a file without an inode number", "path", path)
		os.Exit(-1)
	}

	if ok = ti.Inodes[stat.Ino]; ok {
		ti.log.Debug("We have already seen this i-node, skipping the file", "inodenum", stat.Ino)
		return nil
	}
	ti.Inodes[stat.Ino] = true
	pathlist = append(pathlist, path)
	return nil
}

func (ti treeinfo) String() string {
	r := make([]string, 0, len(ti.Sums))
	for sum, paths := range ti.Sums {
		r = append(r, fmt.Sprintf("%s: %s", sum, strings.Join(paths, " ")))
	}
	return strings.Join(r, "\n")
}

func (ti *treeinfo) checksum(id int, p chan string, wg *sync.WaitGroup) {
	wlog := ti.log.With("workerid", id)
	wlog.Debug("Worker starting")
	defer wg.Done()
	for path := range p {
		f, err := os.Open(path)
		if err != nil {
			wlog.Warn("Could not open file", "path", path, "err", err)
			continue
		}

		h, err := blake2b.New256(nil)
		if err != nil {
			wlog.Error("Could not create new hash", "err", err)
			panic("Exiting")
		}
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
		if ti.progbar != nil {
			err = ti.progbar.Add(1)
			if err != nil {
				panic(err)
			}
		}
	}
	wlog.Debug("Worker exiting")
}

func dedupe(ti *treeinfo) uint64 {
	var savings uint64

	//nolint:staticcheck // We do not use contexts at all
	if ti.log.Enabled(nil, slog.LevelInfo) {
		ti.progbar = progressbar.Default(int64(len(pathlist)), "Cmp/Link")
	}
	for _, names := range ti.Sums {
		if ti.progbar != nil {
			err := ti.progbar.Add(1)
			if err != nil {
				panic(err)
			}
		}
		if len(names) <= 1 {
			continue
		}
		first := names[0]
		fi, err := os.Stat(first)
		if err != nil {
			ti.log.Error("Could not stat destination file for dedupe", "path", first, "error", err)
			os.Exit(-1)
		}
		size := fi.Size()
		for _, name := range names[1:] {
			if *dryrun {
				ti.log.Info("Would deduplicate", "src", name, "dest", first, "size", size)
			} else {
				ti.log.Info("Deduping", "src", name, "dest", first, "size", size)
				tmpname := fmt.Sprintf("%s.tmpdedupe", name)
				err := os.Rename(name, tmpname)
				if err != nil {
					ti.log.Error("Could not rename source file for dedupe", "path", name, "tmpname", tmpname, "error", err)
					os.Exit(-1)
				}
				err = os.Link(first, name)
				if err != nil {
					ti.log.Error("Could not link src file to dest file", "src", name, "dest", first, "error", err)
					os.Exit(-1)
				}
				err = os.Remove(tmpname)
				if err != nil {
					ti.log.Error("Could not delete temp file", "tmpname", tmpname, "error", err)
					os.Exit(-1)
				}
			}
			//nolint:gosec // We _really_ don't expect negative filesizes here,
			// since we already check in the checksumming phase
			savings += uint64(size)
			ti.DupeCount++
		}
	}
	return savings
}
