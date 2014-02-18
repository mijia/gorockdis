package main

import (
    rocks "github.com/tecbot/gorocksdb"
)

func (rh *RocksDBHandler) RedisIncrBy(key, value []byte) ([]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    if key == nil || len(key) == 0 || value == nil || len(value) == 0 {
        return nil, ErrWrongArgumentsCount
    }
    //TODO
    return nil, nil
}

func (rh *RocksDBHandler) RedisGetSet(key, value []byte) ([]byte, error) {
    if data, err := rh.RedisGet(key); err != nil {
        return nil, err
    } else {
        if err := rh.RedisSet(key, value); err != nil {
            return nil, err
        }
        return data, nil
    }
}

func (rh *RocksDBHandler) RedisGet(key []byte) ([]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    if key == nil || len(key) == 0 {
        return nil, ErrWrongArgumentsCount
    }
    options := rocks.NewDefaultReadOptions()
    defer options.Destroy()

    if obj, err := rh.loadRedisObject(options, key); err != nil {
        if err == ErrDoesNotExist {
            return []byte{}, nil
        }
        return nil, err
    } else {
        return obj.Data.([]byte), err
    }
}

func (rh *RocksDBHandler) RedisMget(keys [][]byte) ([][]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    if keys == nil || len(keys) == 0 {
        return nil, ErrWrongArgumentsCount
    }

    options := rocks.NewDefaultReadOptions()
    defer options.Destroy()

    results := make([][]byte, len(keys))
    for i := range results {
        results[i] = []byte{}
    }
    for i := range results {
        if obj, err := rh.loadRedisObject(options, keys[i]); err == nil {
            results[i] = obj.Data.([]byte)
        }
    }
    return results, nil
}

func (rh *RocksDBHandler) RedisSet(key, value []byte) error {
    if rh.db == nil {
        return ErrRocksIsDead
    }
    if key == nil || len(key) == 0 || value == nil || len(value) == 0 {
        return ErrWrongArgumentsCount
    }

    options := rocks.NewDefaultWriteOptions()
    defer options.Destroy()

    return rh.saveRedisObject(options, key, value, kRedisString)
}

func (rh *RocksDBHandler) RedisMset(keyValues [][]byte) error {
    if rh.db == nil {
        return ErrRocksIsDead
    }
    if keyValues == nil || len(keyValues) == 0 || len(keyValues)%2 != 0 {
        return ErrWrongArgumentsCount
    }
    options := rocks.NewDefaultWriteOptions()
    defer options.Destroy()
    for i := 0; i < len(keyValues); i += 2 {
        err := rh.saveRedisObject(options, keyValues[i], keyValues[i+1], kRedisString)
        if err != nil {
            return err
        }
    }
    return nil
}
