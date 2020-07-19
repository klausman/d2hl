package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/dustin/go-humanize"
)

var (
	verbose = flag.Bool("verbose", false, "Be verbose about what's going on")
	quiet   = flag.Bool("quiet", false, "Do not output summary of actions")
)

type treeinfo struct {
	Sums      map[string][]string
	Inodes    map[uint64]bool
	DupeCount int
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
	if info.IsDir() {
		return nil
	}
	if strings.HasSuffix(path, ".tmdedupe") {
		panic(fmt.Sprintf(
			"'%s' indicates previous failed run, please investigate", path))
	}
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
	sum, err := checksum(path)
	if err != nil {
		return err
	}
	ti.Sums[sum] = append(ti.Sums[sum], path)
	if len(ti.Sums[sum]) > 1 {
		ti.DupeCount += 1
	}
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

func checksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
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
			if *verbose {
				fmt.Fprintf(os.Stderr, "Deduping %s to %s\n", name, first)
			}
			tmpname := fmt.Sprintf("%s.tmdedupe", name)
			err := os.Rename(name, tmpname)
			check(err)
			err = os.Link(first, name)
			check(err)
			err = os.Remove(tmpname)
			check(err)
			savings += size
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
	if !*quiet {
		fmt.Printf("Found %d dedupe-able files\n", ti.DupeCount)
	}
	s := dedupe(&ti)
	if !*quiet {
		fmt.Printf("Saved %s bytes of disk space\n", humanize.Bytes(uint64(s)))
	}
}
