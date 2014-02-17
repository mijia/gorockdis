package main

import (
    "code.google.com/p/gcfg"
    "flag"
    "fmt"
    "log"
    "math/rand"
    "os"
    "os/signal"
    "runtime"
    "syscall"
    "time"
)

type Config struct {
    Server struct {
        Bind       string
        Port       int
        MonitorLog bool
    }
    Database struct {
        DbDir           string
        MaxMemory       string
        CreateIfMissing bool
        BloomFilter     int
        Compression     string
        CompactionStyle string
        MaxOpenFiles    int
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
    defer func() {
        rock.close()
        server.Close()
    }()

    signalChan := make(chan os.Signal, 1)
    signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT)
    go func() {
        s := <-signalChan
        log.Printf("[Main] Captured the signal %v", s)
        rock.close()
        server.Close()
        os.Exit(0)
    }()

    if err := server.RegisterHandler(rock); err != nil {
        log.Fatalf("[Main] Register Handler error, %s", err)
    }
    if err := server.ListenAndServe(); err != nil {
        log.Fatalf("[Main] ListenAndServe error, %s", err)
    }
}

func init() {
    log.SetFlags(log.LstdFlags | log.Lshortfile)
    runtime.GOMAXPROCS(runtime.NumCPU())
    rand.Seed(time.Now().UnixNano())
}

var _ = fmt.Println
