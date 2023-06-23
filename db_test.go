package godb

import (
	"fmt"
	"io/ioutil"
	"os"
)

func mustTempDir() (string, func()) {
	dir, err := ioutil.TempDir("", "db-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir, func() { os.RemoveAll(dir) }
}

func mustTempFile() (string, func()) {
	file, err := os.CreateTemp("", "db-test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	defer file.Close()
	return file.Name(), func() { os.Remove(file.Name()) }
}
