package main

import (
    "log"
)

func NewRocksDBHandler(config Config) *RocksDBHandler {
    handler := &RocksDBHandler{}
    handler.dbDir = config.Database.Dbdir
    handler.dbCount = config.Database.Databases
    cacheSize, err := parseComputerSize(config.Database.Maxmemory)
    if err != nil {
        log.Fatalf("[Config] Format error for [Database] maxmemory=%s", config.Database.Maxmemory)
    }
    handler.cacheSize = cacheSize

    if err := handler.Init(); err != nil {
        log.Fatal(err)
    }
    return handler
}

type RocksDBHandler struct {
    dbDir     string
    dbCount   int
    cacheSize int64
}

func (rock *RocksDBHandler) Init() error {
    return nil
}

func (rock *RocksDBHandler) Info() ([]byte, error) {
    return []byte("TBD\r\n"), nil
}

func (rock *RocksDBHandler) Get(key []byte) ([]byte, error) {
    return []byte(string(key) + "/RocksDB Rocks"), nil
}

func (rock *RocksDBHandler) Mget(keys [][]byte) ([][]byte, error) {
    results := make([][]byte, len(keys))
    for i := range results {
        results[i] = []byte(string(keys[i]) + "/RocksDB Rocks")
    }
    return results, nil
}

func (rock *RocksDBHandler) Set(key, value []byte) error {
    return nil
}

func (rock *RocksDBHandler) Del(key []byte, keys ...[]byte) (int, error) {
    keyData := append([][]byte{key}, keys...)
    return len(keyData), nil
}

func (rock *RocksDBHandler) Select(db int) error {
    return nil
}

func (rock *RocksDBHandler) Ping() (*StatusReply, error) {
    return &StatusReply{"PONG"}, nil
}

// Maybe support those,
// Keys: EXISTS, DUMP(snapshot), EXPIRE, KEYS, SCAN
// Strings: MSET, DECR, DECRBY, INCR, INCRBY
// Server: CLIENT LIST, DBSIZE, TIME
