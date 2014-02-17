package main

import (
    "code.google.com/p/gcfg"
    "flag"
    "fmt"
    "log"
    "math/rand"
    "runtime"
    "time"
)

type Config struct {
    Server struct {
        Bind string
        Port int
    }
    Database struct {
        Dbdir     string
        Databases int
        Maxmemory string
    }
}

func main() {
    var confName string
    flag.StringVar(&confName, "conf", "rockdis.conf", "Rockdis Configuration file")
    flag.Parse()

    var config Config
    err := gcfg.ReadFileInto(&config, confName)
    if err != nil {
        log.Fatal(err)
    }

    rock := NewRocksDBHandler(config)
    server := NewServer(config)
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

var _ = fmt.Println
