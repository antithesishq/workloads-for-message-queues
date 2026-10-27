package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
)

func openStateFile(path string) ([]string, *os.File) {
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		log.Fatalf("read state file %s: %v", path, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		log.Fatalf("open state file %s: %v", path, err)
	}
	complete := data[:bytes.LastIndexByte(data, '\n')+1]
	if err := f.Truncate(int64(len(complete))); err != nil {
		log.Fatalf("truncate state file %s: %v", path, err)
	}
	lines := strings.Split(string(complete), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines, f
}

// fsync so a crash can't drop it
func writeLine(f *os.File, format string, args ...any) {
	if _, err := fmt.Fprintf(f, format+"\n", args...); err != nil {
		log.Fatalf("write state file: %v", err)
	}
	if err := f.Sync(); err != nil {
		log.Fatalf("sync state file: %v", err)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}