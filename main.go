package main

import (
    "log"
    "math/rand"
    "runtime"
    "time"
)

func init() {
    log.SetFlags(log.LstdFlags)
    runtime.GOMAXPROCS(runtime.NumCPU())
    rand.Seed(time.Now().UnixNano())
}

func main() {
    server := &Server{Address: ":6379"}
    if err := server.ListenAndServe(); err != nil {
        panic(err)
    }
}
