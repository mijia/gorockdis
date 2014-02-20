package main

import (
    "fmt"
    "os"
    "runtime"
    "strings"
    "time"
)

// Server and Connection Command
func (rh *RocksDBHandler) RedisSelect(db int) error {
    if rh.db == nil {
        return ErrRocksIsDead
    }
    return nil
}

func (rh *RocksDBHandler) RedisPing() (*StatusReply, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    return &StatusReply{"PONG"}, nil
}

func (rh *RocksDBHandler) RedisInfo() ([]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }

    data := make([]string, 0)

    // server section
    qpsStart := globalStat.qpsStart.Get()
    qps := float64(globalStat.qpsCommands.Get()) / float64(time.Now().Unix()-qpsStart)
    serverSection := []string{
        "# Server",
        "version: " + globalStat.version,
        "os: " + runtime.GOOS,
        fmt.Sprintf("process_id: %d", os.Getpid()),
        fmt.Sprintf("tcp_port: %d", globalStat.config.Server.Port),
        "config_file: " + globalStat.configFile,
        fmt.Sprintf("uptime: %s", time.Since(globalStat.startTime)),
        fmt.Sprintf("connected_clients: %d", globalStat.clients.Get()),
        fmt.Sprintf("total_connections_received: %d", globalStat.totalConnections.Get()),
        fmt.Sprintf("total_commands_processed: %d", globalStat.totalCommands.Get()),
        fmt.Sprintf("instantaneous_ops_per_sec: %v", qps),
        fmt.Sprintf("keyspace_hits: %d", globalStat.keyHits.Get()),
        fmt.Sprintf("keyspace_misses: %d", globalStat.keyMisses.Get()),
        "",
    }
    data = append(data, serverSection...)

    // Runtime Section
    var memStats runtime.MemStats
    runtime.ReadMemStats(&memStats)
    runtimeSection := []string{
        "# Runtime",
        fmt.Sprintf("cpu_count: %d", runtime.NumCPU()),
        fmt.Sprintf("cgo_calls: %d", runtime.NumCgoCall()),
        fmt.Sprintf("goroutine_count: %d", runtime.NumGoroutine()),
        fmt.Sprintf("mem_alloc: %d", memStats.Alloc),
        fmt.Sprintf("mem_total: %d", memStats.TotalAlloc),
        fmt.Sprintf("mem_stack: %d", memStats.StackInuse),
        fmt.Sprintf("mem_heap_alloc: %d", memStats.HeapAlloc),
        fmt.Sprintf("mem_heap_objects: %d", memStats.HeapObjects),
        fmt.Sprintf("mem_gc_count: %d", memStats.NumGC),
        fmt.Sprintf("mem_gc_total_pause: %v", float64(memStats.PauseTotalNs)/float64(time.Millisecond)),
        "",
    }
    data = append(data, runtimeSection...)

    // rocksdb section
    rocksSection := []string{
        "# Rocksdb Config",
        "rocksdb_directory: " + globalStat.config.Database.DbDir,
        "max_memory: " + globalStat.config.Database.MaxMemory,
        "block_size: " + globalStat.config.Database.BlockSize,
        "compression: " + globalStat.config.Database.Compression,
        "compaction_style: " + globalStat.config.Database.CompactionStyle,
        fmt.Sprintf("max_memtable_merge: %d", globalStat.config.Database.MaxMerge),
        "",
        "# Rocksdb Stats",
    }
    rocksStats := strings.Split(rh.db.GetProperty("rocksdb.stats"), "\n")
    rocksStats = rocksStats[len(rocksStats)-14:]
    for _, rockStat := range rocksStats {
        rocksSection = append(rocksSection, rockStat)
    }
    rocksSection = append(rocksSection, "")
    data = append(data, rocksSection...)

    return []byte(strings.Join(data, "\r\n")), nil
}

var _ = fmt.Println
