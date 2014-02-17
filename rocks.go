package main

import (
    "fmt"
    rocks "github.com/mijia/gorocks"
    "log"
    "strings"
)

func NewRocksDBHandler(config Config) *RocksDBHandler {
    handler := &RocksDBHandler{}
    handler.dbDir = config.Database.DbDir
    cacheSize, err := parseComputerSize(config.Database.MaxMemory)
    if err != nil {
        log.Fatalf("[Config] Format error for [Database] maxmemory=%s", config.Database.MaxMemory)
    }
    handler.cacheSize = cacheSize
    handler.createIfMissing = config.Database.CreateIfMissing
    handler.bloomFilter = config.Database.BloomFilter
    handler.compression = config.Database.Compression
    handler.compactionStyle = config.Database.CompactionStyle
    handler.maxOpenFiles = config.Database.MaxOpenFiles

    if err := handler.init(); err != nil {
        log.Fatal(err)
    }
    return handler
}

type RocksDBHandler struct {
    dbDir           string
    cacheSize       int
    createIfMissing bool
    bloomFilter     int
    compression     string
    compactionStyle string
    maxOpenFiles    int

    options *rocks.Options
    db      *rocks.DB
}

func (rh *RocksDBHandler) init() error {
    rh.options = rocks.NewOptions()
    rh.options.SetCache(rocks.NewLRUCache(rh.cacheSize))
    rh.options.SetCreateIfMissing(rh.createIfMissing)
    if rh.bloomFilter > 0 {
        rh.options.SetFilterPolicy(rocks.NewBloomFilter(rh.bloomFilter))
    }
    if rh.maxOpenFiles > 0 {
        rh.options.SetMaxOpenFiles(rh.maxOpenFiles)
    }
    if rh.compression == "snappy" {
        rh.options.SetCompression(rocks.SnappyCompression)
    } else {
        rh.options.SetCompression(rocks.NoCompression)
    }
    switch rh.compactionStyle {
    case "level":
        rh.options.SetCompactionStyle(rocks.LevelStyleCompaction)
    case "universal":
        rh.options.SetCompactionStyle(rocks.UniversalStyleCompaction)
    }

    db, err := rocks.Open(rh.dbDir, rh.options)
    if err != nil {
        rh.close()
        return err
    }
    rh.db = db

    infos := []string{
        fmt.Sprintf("dbDir=%s", rh.dbDir),
        fmt.Sprintf("cacheSize=%d", rh.cacheSize),
        fmt.Sprintf("createIfMissing=%v", rh.createIfMissing),
        fmt.Sprintf("bloomFilter=%d", rh.bloomFilter),
        fmt.Sprintf("compression=%s", rh.compression),
        fmt.Sprintf("compactionStyle=%s", rh.compactionStyle),
        fmt.Sprintf("maxOpenFiles=%d", rh.maxOpenFiles),
    }
    log.Printf("[RocksDBHandler] Inited, %s", strings.Join(infos, ", "))
    return nil
}

func (rh *RocksDBHandler) close() {
    if rh.options != nil {
        rh.options.Close()
    }
    if rh.db != nil {
        rh.db.Close()
    }
    log.Printf("[RocksDBHandler] Closed.")
}

var (
    ErrRocksIsDead = fmt.Errorf("RocksDB is dead")
)

func (rh *RocksDBHandler) Info() ([]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    return []byte("TBD\r\n"), nil
}

func (rh *RocksDBHandler) Get(key []byte) ([]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    if key == nil || len(key) == 0 {
        return nil, fmt.Errorf("wrong number of arguments for 'get' command")
    }
    ro := rocks.NewReadOptions()
    defer ro.Close()
    return rh.db.Get(ro, key)
}

func (rh *RocksDBHandler) Mget(keys [][]byte) ([][]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    if keys == nil || len(keys) == 0 {
        return nil, fmt.Errorf("wrong number of arguments for 'mget' command")
    }

    ro := rocks.NewReadOptions()
    defer ro.Close()

    results := make([][]byte, len(keys))
    for i := range results {
        if data, err := rh.db.Get(ro, keys[i]); err == nil {
            results[i] = data
        } else {
            results[i] = make([]byte, 0)
            log.Printf("[Mget] Error when accessing rocksdb for key %s, %s", string(keys[i]), err)
        }
    }
    return results, nil
}

func (rh *RocksDBHandler) Set(key, value []byte) error {
    if rh.db == nil {
        return ErrRocksIsDead
    }
    if key == nil || len(key) == 0 || value == nil || len(value) == 0 {
        return fmt.Errorf("wrong number of arguments for 'set' command")
    }

    wo := rocks.NewWriteOptions()
    defer wo.Close()
    return rh.db.Put(wo, key, value)
}

func (rh *RocksDBHandler) Del(key []byte, keys ...[]byte) (int, error) {
    if rh.db == nil {
        return 0, ErrRocksIsDead
    }
    if key == nil || len(key) == 0 {
        return 0, fmt.Errorf("wrong number of arguments for 'del' command")
    }

    keyData := append([][]byte{key}, keys...)
    count := 0
    wo := rocks.NewWriteOptions()
    defer wo.Close()

    for _, dKey := range keyData {
        if err := rh.db.Delete(wo, dKey); err == nil {
            count++
        } else {
            if len(keyData) > 1 {
                return 0, nil
            }
        }
    }
    return count, nil
}

func (rh *RocksDBHandler) Select(db int) error {
    if rh.db == nil {
        return ErrRocksIsDead
    }
    return nil
}

func (rh *RocksDBHandler) Ping() (*StatusReply, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    return &StatusReply{"PONG"}, nil
}

// Maybe support those,
// Keys: EXISTS, DUMP(snapshot), EXPIRE, KEYS, SCAN
// Strings: MSET, DECR, DECRBY, INCR, INCRBY
// Server: CLIENT LIST, DBSIZE, TIME
