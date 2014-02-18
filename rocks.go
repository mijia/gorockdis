package main

import (
    "fmt"
    rocks "github.com/tecbot/gorocksdb"
    "log"
    "strings"
)

func NewRocksDBHandler(config Config) *RocksDBHandler {
    cacheSize, err := parseComputerSize(config.Database.MaxMemory)
    if err != nil {
        log.Fatalf("[Config] Format error for [Database] maxmemory=%s", config.Database.MaxMemory)
    }
    blockSize, err := parseComputerSize(config.Database.BlockSize)
    if err != nil {
        log.Fatalf("[Config] Format error for [Database] blocksize=%s", config.Database.BlockSize)
    }

    handler := &RocksDBHandler{}
    handler.dbDir = config.Database.DbDir
    handler.cacheSize = cacheSize
    handler.blockSize = blockSize
    handler.createIfMissing = config.Database.CreateIfMissing
    handler.bloomFilter = config.Database.BloomFilter
    handler.compression = config.Database.Compression
    handler.compactionStyle = config.Database.CompactionStyle
    handler.maxOpenFiles = config.Database.MaxOpenFiles

    if err := handler.Init(); err != nil {
        log.Fatal(err)
    }
    return handler
}

type RocksDBHandler struct {
    dbDir           string
    cacheSize       int
    blockSize       int
    createIfMissing bool
    bloomFilter     int
    compression     string
    compactionStyle string
    maxOpenFiles    int

    options *rocks.Options
    db      *rocks.DB
}

func (rh *RocksDBHandler) Init() error {
    rh.options = rocks.NewDefaultOptions()
    rh.options.SetBlockCache(rocks.NewLRUCache(rh.cacheSize))
    rh.options.SetBlockSize(rh.blockSize)
    rh.options.SetCreateIfMissing(rh.createIfMissing)
    if rh.bloomFilter > 0 {
        rh.options.SetFilterPolicy(rocks.NewBloomFilter(rh.bloomFilter))
    }
    if rh.maxOpenFiles > 0 {
        rh.options.SetMaxOpenFiles(rh.maxOpenFiles)
    }

    switch rh.compression {
    case "no":
        rh.options.SetCompression(rocks.NoCompression)
    case "snappy":
        rh.options.SetCompression(rocks.SnappyCompression)
    case "zlib":
        rh.options.SetCompression(rocks.ZlibCompression)
    case "bzip2":
        rh.options.SetCompression(rocks.BZip2Compression)
    }

    switch rh.compactionStyle {
    case "level":
        rh.options.SetCompactionStyle(rocks.LevelCompactionStyle)
    case "universal":
        rh.options.SetCompactionStyle(rocks.UniversalCompactionStyle)
    }

    db, err := rocks.OpenDb(rh.options, rh.dbDir)
    if err != nil {
        rh.Close()
        return err
    }
    rh.db = db

    infos := []string{
        fmt.Sprintf("dbDir=%s", rh.dbDir),
        fmt.Sprintf("cacheSize=%d", rh.cacheSize),
        fmt.Sprintf("blockSize=%d", rh.blockSize),
        fmt.Sprintf("createIfMissing=%v", rh.createIfMissing),
        fmt.Sprintf("bloomFilter=%d", rh.bloomFilter),
        fmt.Sprintf("compression=%s", rh.compression),
        fmt.Sprintf("compactionStyle=%s", rh.compactionStyle),
        fmt.Sprintf("maxOpenFiles=%d", rh.maxOpenFiles),
    }
    log.Printf("[RocksDBHandler] Inited, %s", strings.Join(infos, ", "))
    return nil
}

func (rh *RocksDBHandler) Close() {
    if rh.options != nil {
        rh.options.Destroy()
    }
    if rh.db != nil {
        rh.db.Close()
    }
    log.Printf("[RocksDBHandler] Closed.")
}

var (
    ErrRocksIsDead = fmt.Errorf("RocksDB is dead")
)

func (rh *RocksDBHandler) RedisInfo() ([]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    return []byte("TBD\r\n"), nil
}

func (rh *RocksDBHandler) copyAndFreeSlice(slice *rocks.Slice) []byte {
    data := make([]byte, slice.Size())
    copy(data, slice.Data())
    slice.Free()
    return data
}

func (rh *RocksDBHandler) RedisGet(key []byte) ([]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    if key == nil || len(key) == 0 {
        return nil, fmt.Errorf("wrong number of arguments for 'get' command")
    }
    ro := rocks.NewDefaultReadOptions()
    defer ro.Destroy()

    slice, err := rh.db.Get(ro, key)
    return rh.copyAndFreeSlice(slice), err
}

func (rh *RocksDBHandler) RedisMget(keys [][]byte) ([][]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    if keys == nil || len(keys) == 0 {
        return nil, fmt.Errorf("wrong number of arguments for 'mget' command")
    }

    ro := rocks.NewDefaultReadOptions()
    defer ro.Destroy()

    results := make([][]byte, len(keys))
    for i := range results {
        if slice, err := rh.db.Get(ro, keys[i]); err == nil {
            results[i] = rh.copyAndFreeSlice(slice)
        } else {
            results[i] = make([]byte, 0)
            log.Printf("[Mget] Error when accessing rocksdb for key %s, %s", string(keys[i]), err)
        }
    }
    return results, nil
}

func (rh *RocksDBHandler) RedisSet(key, value []byte) error {
    if rh.db == nil {
        return ErrRocksIsDead
    }
    if key == nil || len(key) == 0 || value == nil || len(value) == 0 {
        return fmt.Errorf("wrong number of arguments for 'set' command")
    }

    wo := rocks.NewDefaultWriteOptions()
    defer wo.Destroy()

    return rh.db.Put(wo, key, value)
}

func (rh *RocksDBHandler) RedisDel(key []byte, keys ...[]byte) (int, error) {
    if rh.db == nil {
        return 0, ErrRocksIsDead
    }
    if key == nil || len(key) == 0 {
        return 0, fmt.Errorf("wrong number of arguments for 'del' command")
    }

    keyData := append([][]byte{key}, keys...)
    count := 0
    wo := rocks.NewDefaultWriteOptions()
    defer wo.Destroy()

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

// Maybe support those,
// Keys: EXISTS, DUMP(snapshot), EXPIRE, KEYS, SCAN
// Strings: MSET, DECR, DECRBY, INCR, INCRBY
// Server: CLIENT LIST, DBSIZE, TIME
