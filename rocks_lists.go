package main

import (
    "encoding/gob"
    "fmt"
    rocks "github.com/tecbot/gorocksdb"
    "reflect"
)

func (rh *RocksDBHandler) RedisLlen(key []byte) (int, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisList); err != nil {
        return 0, err
    }

    data, err := rh._list_getData(key)
    if err != nil {
        return 0, err
    }
    return len(data), nil
}

func (rh *RocksDBHandler) RedisLindex(key []byte, index int) ([]byte, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisList); err != nil {
        return nil, err
    }

    data, err := rh._list_getData(key)
    if err != nil {
        return nil, err
    }
    if index < 0 {
        index += len(data)
    }
    if index < 0 || index >= len(data) {
        return []byte{}, nil
    }
    return data[index], nil
}

func (rh *RocksDBHandler) RedisLrange(key []byte, start, end int) ([][]byte, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisList); err != nil {
        return nil, err
    }

    data, err := rh._list_getData(key)
    if err != nil {
        return nil, err
    }
    if len(data) == 0 {
        return [][]byte{}, nil
    }
    start = rh._list_getIndex(start, len(data), false)
    end = rh._list_getIndex(end, len(data), true)
    return data[start:end], nil
}

func (rh *RocksDBHandler) RedisLpop(key []byte) ([]byte, error) {
    return rh._list_Pop(0, key)
}

func (rh *RocksDBHandler) RedisRpop(key []byte) ([]byte, error) {
    return rh._list_Pop(-1, key)
}

func (rh *RocksDBHandler) RedisRpush(key, value []byte, values ...[]byte) (int, error) {
    return rh._list_Push(-1, key, value, values...)
}

func (rh *RocksDBHandler) RedisLpush(key, value []byte, values ...[]byte) (int, error) {
    return rh._list_Push(0, key, value, values...)
}

func (rh *RocksDBHandler) _list_getIndex(index, length int, isRightmost bool) int {
    if index < 0 {
        index += length
        if index < 0 {
            if isRightmost {
                index = -1
            } else {
                index = 0
            }
        }
    }
    if isRightmost {
        index++
    }
    if index > length {
        index = length
    }
    return index
}

func (rh *RocksDBHandler) _list_Pop(direction int, key []byte) ([]byte, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisList); err != nil {
        return nil, err
    }

    data, err := rh._list_getData(key)
    if err != nil {
        return nil, err
    }
    if len(data) == 0 {
        return []byte{}, nil // this is not an error
    }
    popData := data[0]
    if direction == -1 {
        popData = data[len(data)-1]
    }
    if err := rh._list_doMerge(key, popData, kListOpRemove, direction); err != nil {
        return nil, err
    }
    return popData, nil
}

func (rh *RocksDBHandler) _list_Push(direction int, key, value []byte, values ...[]byte) (int, error) {
    if err := rh.checkRedisCall(key, value); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisList); err != nil {
        return 0, err
    }
    values = append([][]byte{value}, values...)
    if err := rh._list_doMerge(key, values, kListOpInsert, direction); err != nil {
        return 0, err
    }
    if data, err := rh._list_getData(key); err == nil {
        return len(data), nil
    } else {
        return 0, err
    }
}

func (rh *RocksDBHandler) _list_doMerge(key []byte, value interface{}, opCode string, index int) error {
    var values [][]byte
    if d1Slice, ok := value.([]byte); ok {
        values = [][]byte{d1Slice}
    }
    if d2Slice, ok := value.([][]byte); ok {
        values = d2Slice
    }
    if values == nil || len(values) == 0 {
        return ErrWrongArgumentsCount
    }

    options := rocks.NewDefaultWriteOptions()
    defer options.Destroy()
    batch := rocks.NewWriteBatch()
    defer batch.Destroy()
    batch.Put(rh.getTypeKey(key), []byte(kRedisList))
    for _, dValue := range values {
        operand := ListOperand{opCode, index, dValue}
        if data, err := encode(operand); err == nil {
            batch.Merge(key, data)
        } else {
            return err
        }
    }
    return rh.db.Write(options, batch)
}

func (rh *RocksDBHandler) _list_getData(key []byte) ([][]byte, error) {
    options := rocks.NewDefaultReadOptions()
    defer options.Destroy()
    if obj, err := rh.loadRedisObject(options, key); err != nil {
        if err == ErrDoesNotExist {
            return [][]byte{}, nil
        }
        return nil, err
    } else {
        if obj.Type != kRedisList {
            return nil, ErrWrongTypeRedisObject
        }
        return obj.Data.([][]byte), nil
    }
}

const (
    kListOpInsert = "insert"
    kListOpRemove = "remove"
)

type ListOperand struct {
    Command string
    Index   int
    Data    []byte
}

func init() {
    gob.Register(&ListOperand{})
}

type ListMerger struct{}

func (m *ListMerger) FullMerge(existingObject *RedisObject, operands [][]byte) bool {
    listData, ok := existingObject.Data.([][]byte)
    if !ok {
        listData = [][]byte{}
    }
    fmt.Println("Before", listData)
    for _, operand := range operands {
        if obj, err := decode(operand, reflect.TypeOf(ListOperand{})); err == nil {
            op := obj.(ListOperand)
            switch op.Command {
            case kListOpInsert:
                if op.Index == 0 {
                    listData = append([][]byte{op.Data}, listData...)
                } else {
                    listData = append(listData, op.Data)
                }
            case kListOpRemove:
                if len(listData) > 0 {
                    if op.Index == 0 {
                        listData = listData[1:]
                    } else {
                        listData = listData[:len(listData)-1]
                    }
                }
            }
        }
    }
    fmt.Println("After", listData)
    existingObject.Data = listData
    return true
}

func (m *ListMerger) PartialMerge(leftOperand, rightOperand []byte) ([]byte, bool) {
    return nil, false
}

var _ = fmt.Println
