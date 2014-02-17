package main

import (
    "log"
    "math/rand"
    "runtime"
    "time"
)

func main() {
    rock := &RocksDBHandler{}
    server := NewServer()
    if err := server.RegisterHandler(rock); err != nil {
        log.Fatal(err)
    }
    if err := server.ListenAndServe(); err != nil {
        log.Fatal(err)
    }
}

func init() {
    log.SetFlags(log.LstdFlags)
    runtime.GOMAXPROCS(runtime.NumCPU())
    rand.Seed(time.Now().UnixNano())
}
