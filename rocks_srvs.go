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

    // rocksdb section
    rocksSection := []string{
        "# Rocksdb",
        "rocksdb_directory: " + globalStat.config.Database.DbDir,
        "max_memory: " + globalStat.config.Database.MaxMemory,
        "block_size: " + globalStat.config.Database.BlockSize,
        "compression: " + globalStat.config.Database.Compression,
        "compaction_style: " + globalStat.config.Database.CompactionStyle,
        fmt.Sprintf("max_memtable_merge: %d", globalStat.config.Database.MaxMerge),
        "",
        "# Rocksdb Internal",
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
