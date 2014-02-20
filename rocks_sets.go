package main

import (
    "encoding/gob"
    "fmt"
    rocks "github.com/tecbot/gorocksdb"
    "reflect"
)

func (rh *RocksDBHandler) RedisScard(key []byte) (int, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisSet); err != nil {
        return 0, err
    }

    setData, err := rh._set_getData(key)
    if err != nil {
        return 0, err
    }
    return len(setData), nil
}

func (rh *RocksDBHandler) RedisSismember(key, member []byte) (int, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisSet); err != nil {
        return 0, err
    }

    setData, err := rh._set_getData(key)
    if err != nil {
        return 0, err
    }
    if _, ok := setData[string(member)]; ok {
        return 1, nil
    }
    return 0, nil
}

func (rh *RocksDBHandler) RedisSmembers(key []byte) ([][]byte, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisSet); err != nil {
        return nil, err
    }

    setData, err := rh._set_getData(key)
    if err != nil {
        return nil, err
    }
    return __setToBytes(setData), nil
}

func (rh *RocksDBHandler) RedisSadd(key, value []byte, values ...[]byte) (int, error) {
    totalCount, existCount, err := rh._set_doMembership(kSetOpSet, key, value, values...)
    if err != nil {
        return 0, err
    }
    return totalCount - existCount, nil
}

func (rh *RocksDBHandler) RedisSrem(key, value []byte, values ...[]byte) (int, error) {
    _, existCount, err := rh._set_doMembership(kSetOpDelete, key, value, values...)
    if err != nil {
        return 0, err
    }
    return existCount, nil
}

func (rh *RocksDBHandler) _set_doMembership(opCode string, key, value []byte, values ...[]byte) (int, int, error) {
    if err := rh.checkRedisCall(key, value); err != nil {
        return 0, 0, err
    }
    if err := rh.checkKeyType(key, kRedisSet); err != nil {
        return 0, 0, err
    }

    setData, err := rh._set_getData(key)
    if err != nil {
        return 0, 0, err
    }
    existCount := 0
    values = append([][]byte{value}, values...)
    for i := range values {
        if _, ok := setData[string(values[i])]; ok {
            existCount++
        }
    }
    if err := rh._set_doMerge(key, values, opCode); err != nil {
        return 0, 0, err
    }
    return len(values), existCount, nil
}

func (rh *RocksDBHandler) _set_doMerge(key []byte, values [][]byte, opCode string) error {
    if values == nil || len(values) == 0 {
        return ErrWrongArgumentsCount
    }
    options := rocks.NewDefaultWriteOptions()
    defer options.Destroy()
    batch := rocks.NewWriteBatch()
    defer batch.Destroy()
    batch.Put(rh.getTypeKey(key), []byte(kRedisSet))
    for _, value := range values {
        operand := SetOperand{opCode, value}
        if data, err := encode(operand); err == nil {
            batch.Merge(key, data)
        } else {
            return err
        }
    }
    return rh.db.Write(options, batch)
}

func (rh *RocksDBHandler) _set_getData(key []byte) (map[string]bool, error) {
    setData := make(map[string]bool)
    options := rocks.NewDefaultReadOptions()
    defer options.Destroy()
    if obj, err := rh.loadRedisObject(options, key); err != nil {
        if err == ErrDoesNotExist {
            return setData, nil
        }
        return nil, err
    } else {
        if obj.Type != kRedisSet {
            return nil, ErrWrongTypeRedisObject
        }
        data := obj.Data.([][]byte)
        for _, itemData := range data {
            setData[string(itemData)] = true
        }
        return setData, nil
    }
}

const (
    kSetOpSet    = "set"
    kSetOpDelete = "delete"
)

type SetOperand struct {
    Command string
    Key     []byte
}

func init() {
    gob.Register(&SetOperand{})
}

type SetMerger struct{}

func (m *SetMerger) FullMerge(existingObject *RedisObject, operands [][]byte) bool {
    rawData, ok := existingObject.Data.([][]byte)
    if !ok {
        rawData = [][]byte{}
    }
    setData := make(map[string]bool)
    for i := range rawData {
        setData[string(rawData[i])] = true
    }
    for _, operand := range operands {
        if obj, err := decode(operand, reflect.TypeOf(SetOperand{})); err == nil {
            op := obj.(SetOperand)
            switch op.Command {
            case kSetOpSet:
                setData[string(op.Key)] = true
            case kSetOpDelete:
                delete(setData, string(op.Key))
            }
        }
    }
    existingObject.Data = __setToBytes(setData)
    return true
}

func (m *SetMerger) PartialMerge(leftOperand, rightOperand []byte) ([]byte, bool) {
    obj, err := decode(leftOperand, reflect.TypeOf(SetOperand{}))
    if err != nil {
        return nil, false
    }
    leftOp := obj.(SetOperand)
    obj, err = decode(rightOperand, reflect.TypeOf(SetOperand{}))
    if err != nil {
        return nil, false
    }
    rightOp := obj.(SetOperand)
    if string(leftOp.Key) == string(rightOp.Key) {
        return rightOperand, true
    }

    return nil, false
}

func __setToBytes(setData map[string]bool) [][]byte {
    rawData := make([][]byte, len(setData))
    index := 0
    for key := range setData {
        rawData[index] = []byte(key)
        index++
    }
    return rawData
}

var _ = fmt.Println
