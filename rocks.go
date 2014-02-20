package main

import (
    "encoding/gob"
    "fmt"
    rocks "github.com/tecbot/gorocksdb"
    "log"
    "reflect"
    "strings"
)

const (
    kRedisString = "string"
    kRedisList   = "list"
    kRedisHash   = "hash"
    kRedisSet    = "set"
)

var (
    kTypeKeyPrefix = []byte("__*type*__")
)

type RedisObject struct {
    Type string
    Data interface{}
}

func init() {
    gob.Register(&RedisObject{})
    gob.Register([][]byte{})
}

type DataStructureMerger interface {
    FullMerge(existingObject *RedisObject, operands [][]byte) bool
    PartialMerge(leftOperand, rightOperand []byte) ([]byte, bool)
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
    handler.maxMerge = config.Database.MaxMerge

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
    maxMerge        int

    options *rocks.Options
    db      *rocks.DB

    dsMergers map[string]DataStructureMerger
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

    rh.dsMergers = make(map[string]DataStructureMerger)
    rh.dsMergers[kRedisString] = &StringMerger{}
    rh.dsMergers[kRedisList] = &ListMerger{}
    rh.dsMergers[kRedisHash] = &HashMerger{}
    rh.dsMergers[kRedisSet] = &SetMerger{}

    if rh.maxMerge > 0 {
        rh.options.SetMaxSuccessiveMerges(rh.maxMerge)
    }
    rh.options.SetMergeOperator(rocks.NewMergeOperator(rh))

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
        fmt.Sprintf("maxMerge=%d", rh.maxMerge),
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

func (rh *RocksDBHandler) getTypeKey(key []byte) []byte {
    return append(kTypeKeyPrefix, key...)
}

func (rh *RocksDBHandler) getKeyType(key []byte) (string, error) {
    if rh.db == nil {
        return "", ErrRocksIsDead
    }
    if key == nil || len(key) == 0 {
        return "", ErrWrongArgumentsCount
    }

    options := rocks.NewDefaultReadOptions()
    if slice, err := rh.db.Get(options, rh.getTypeKey(key)); err == nil {
        defer slice.Free()
        return string(slice.Data()), nil
    } else {
        return "", err
    }
}

func (rh *RocksDBHandler) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
    var redisObj RedisObject
    keyType, err := rh.getKeyType(key)
    if err != nil || keyType == "" {
        return nil, false
    }
    var emptyData interface{}
    switch keyType {
    case kRedisString:
        emptyData = []byte{}
    default:
        emptyData = [][]byte{}
    }

    if existingValue == nil || len(existingValue) == 0 {
        redisObj = RedisObject{keyType, emptyData}
    } else {
        if obj, err := decode(existingValue, reflect.TypeOf(redisObj)); err != nil {
            return nil, false
        } else {
            redisObj = obj.(RedisObject)
        }
        if redisObj.Type != keyType {
            return nil, false
        }
    }

    if merger, ok := rh.dsMergers[keyType]; ok {
        merged := merger.FullMerge(&redisObj, operands)
        if !merged {
            return nil, false
        }
        if data, err := encode(redisObj); err == nil {
            return data, true
        }
    }

    return nil, false
}

func (rh *RocksDBHandler) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
    keyType, err := rh.getKeyType(key)
    if err != nil {
        return nil, false
    }
    if merger, ok := rh.dsMergers[keyType]; ok {
        return merger.PartialMerge(leftOperand, rightOperand)
    }
    return nil, false
}

func (rh *RocksDBHandler) Name() string {
    return "GoRockdisMergeOperator"
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
    slice, err := rh.db.Get(options, key)
    if err != nil {
        log.Printf("[loadRedisObject] Error when GET < RocksDB, %s", err)
        return RedisObject{}, err
    }

    data := rh.copySlice(slice, true)
    if data == nil || len(data) == 0 {
        return RedisObject{}, ErrDoesNotExist
    }

    if obj, err := decode(data, reflect.TypeOf(RedisObject{})); err == nil {
        return obj.(RedisObject), nil
    } else {
        return RedisObject{}, err
    }
}

func (rh *RocksDBHandler) saveRedisObject(options *rocks.WriteOptions, key []byte, value interface{}, objType string) error {
    obj := RedisObject{
        Type: objType,
        Data: value,
    }
    data, err := encode(obj)
    if err != nil {
        return err
    }

    batch := rocks.NewWriteBatch()
    defer batch.Destroy()
    batch.Put(rh.getTypeKey(key), []byte(objType))
    batch.Put(key, data)
    err = rh.db.Write(options, batch)
    if err != nil {
        log.Printf("[saveRedisObject] Error when PUT > RocksDB, %s", err)
    }
    return err
}

func (rh *RocksDBHandler) checkRedisCall(args ...[]byte) error {
    if rh.db == nil {
        return ErrRocksIsDead
    }
    if len(args) > 0 {
        for _, arg := range args {
            if arg == nil || len(arg) == 0 {
                return ErrWrongArgumentsCount
            }
        }
    }
    return nil
}

func (rh *RocksDBHandler) checkKeyType(key []byte, assertType string) error {
    if keyType, err := rh.getKeyType(key); err != nil {
        return err
    } else {
        if keyType != "" && keyType != assertType {
            return ErrWrongTypeRedisObject
        }
    }
    return nil
}
