package main

import (
    "bytes"
    "encoding/gob"
    "fmt"
    rocks "github.com/tecbot/gorocksdb"
    "log"
    "strings"
)

const (
    kRedisString = "string"
    kRedisList   = "list"
    kRedisHash   = "hash"
)

type RedisObject struct {
    Type string
    Data interface{}
}

func init() {
    gob.Register(&RedisObject{})
}

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
    ErrRocksIsDead          = fmt.Errorf("RocksDB is dead")
    ErrDoesNotExist         = fmt.Errorf("There is no such object")
    ErrWrongArgumentsCount  = fmt.Errorf("Wrong number of arguments for command")
    ErrWrongTypeRedisObject = fmt.Errorf("Operation against a key holding the wrong kind of value")
    ErrNotNumber            = fmt.Errorf("value is not an integer or out of range")
)

func (rh *RocksDBHandler) copySlice(slice *rocks.Slice, toFree bool) []byte {
    data := make([]byte, slice.Size())
    copy(data, slice.Data())
    if toFree {
        slice.Free()
    }
    return data
}

func (rh *RocksDBHandler) loadRedisObject(options *rocks.ReadOptions, key []byte) (RedisObject, error) {
    empty := RedisObject{}
    slice, err := rh.db.Get(options, key)
    if err != nil {
        log.Printf("[loadRedisObject] Error when GET < RocksDB, %s", err)
        return empty, err
    }

    data := rh.copySlice(slice, true)
    if data == nil || len(data) == 0 {
        return empty, ErrDoesNotExist
    }

    var obj RedisObject
    buffer := bytes.NewBuffer(data)
    decoder := gob.NewDecoder(buffer)
    if err := decoder.Decode(&obj); err != nil {
        log.Printf("[loadRedisObject] Error when decode object from key[%s], %s", string(key), err)
        return empty, err
    }
    return obj, nil
}

func (rh *RocksDBHandler) saveRedisObject(options *rocks.WriteOptions, key []byte, value interface{}, objType string) error {
    obj := RedisObject{
        Type: objType,
        Data: value,
    }
    buffer := new(bytes.Buffer)
    encoder := gob.NewEncoder(buffer)
    if err := encoder.Encode(obj); err != nil {
        return err
    }
    err := rh.db.Put(options, key, buffer.Bytes())
    if err != nil {
        log.Printf("[saveRedisObject] Error when PUT > RocksDB, %s", err)
    }
    return err
}
