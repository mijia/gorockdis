package main

import (
    "encoding/gob"
    "fmt"
    rocks "github.com/tecbot/gorocksdb"
    "reflect"
)

func (rh *RocksDBHandler) RedisHkeys(key []byte) ([][]byte, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return nil, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return nil, err
    }
    data, index := make([][]byte, len(hashData)), 0
    for key := range hashData {
        data[index] = []byte(key)
        index++
    }
    return data, nil
}

func (rh *RocksDBHandler) RedisHvals(key []byte) ([][]byte, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return nil, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return nil, err
    }
    data, index := make([][]byte, len(hashData)), 0
    for _, value := range hashData {
        data[index] = value
        index++
    }
    return data, nil
}

func (rh *RocksDBHandler) RedisHlen(key []byte) (int, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return 0, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return 0, err
    }
    return len(hashData), nil
}

func (rh *RocksDBHandler) RedisHdel(key, field []byte, fields ...[]byte) (int, error) {
    if err := rh.checkRedisCall(key, field); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return 0, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return 0, err
    }

    allFields := append([][]byte{field}, fields...)
    count := 0
    data := make([][]byte, 0)
    for _, f := range allFields {
        if value, ok := hashData[string(f)]; ok {
            count++
            data = append(data, [][]byte{f, value}...)
        }
    }
    if count > 0 {
        if err := rh._hash_doMerge(key, data, kHashOpDelete); err != nil {
            return 0, err
        }
    }
    return count, nil
}

func (rh *RocksDBHandler) RedisHexists(key, field []byte) (int, error) {
    if err := rh.checkRedisCall(key, field); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return 0, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return 0, err
    }
    _, ok := hashData[string(field)]
    if ok {
        return 1, nil
    }
    return 0, nil
}

func (rh *RocksDBHandler) RedisHget(key, field []byte) ([]byte, error) {
    if err := rh.checkRedisCall(key, field); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return nil, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return nil, err
    }
    if value, ok := hashData[string(field)]; ok {
        return value, nil
    } else {
        return nil, nil
    }
}

func (rh *RocksDBHandler) RedisHmget(key, field []byte, fields ...[]byte) ([][]byte, error) {
    if err := rh.checkRedisCall(key, field); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return nil, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return nil, err
    }
    allFields := append([][]byte{field}, fields...)
    data := make([][]byte, len(allFields))
    for i, f := range allFields {
        if value, ok := hashData[string(f)]; ok {
            data[i] = value
        } else {
            data[i] = nil
        }
    }
    return data, nil
}

func (rh *RocksDBHandler) RedisHset(key, field, value []byte) (int, error) {
    if err := rh.checkRedisCall(key, field, value); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return 0, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return 0, err
    }
    _, exists := hashData[string(field)]
    if err := rh._hash_doMerge(key, [][]byte{field, value}, kHashOpSet); err != nil {
        return 0, err
    }
    if exists {
        return 0, nil
    }
    return 1, nil
}

func (rh *RocksDBHandler) RedisHmset(key, field, value []byte, pairs ...[]byte) error {
    if err := rh.checkRedisCall(key, field, value); err != nil {
        return err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return err
    }

    data := append([][]byte{field, value}, pairs...)
    if len(data)%2 != 0 {
        return ErrWrongArgumentsCount
    }
    return rh._hash_doMerge(key, data, kHashOpSet)
}

func (rh *RocksDBHandler) RedisHgetall(key []byte) ([][]byte, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisHash); err != nil {
        return nil, err
    }

    hashData, err := rh._hash_getData(key)
    if err != nil {
        return nil, err
    }
    return __hashToBytes(hashData), nil
}

func (rh *RocksDBHandler) _hash_doMerge(key []byte, values [][]byte, opCode string) error {
    if values == nil || len(values) == 0 || len(values)%2 != 0 {
        return ErrWrongArgumentsCount
    }
    options := rocks.NewDefaultWriteOptions()
    defer options.Destroy()
    batch := rocks.NewWriteBatch()
    defer batch.Destroy()
    batch.Put(rh.getTypeKey(key), []byte(kRedisHash))
    for i := 0; i < len(values); i += 2 {
        operand := HashOperand{opCode, string(values[i]), values[i+1]}
        if data, err := encode(operand); err == nil {
            batch.Merge(key, data)
        } else {
            return err
        }
    }
    return rh.db.Write(options, batch)
}

func (rh *RocksDBHandler) _hash_getData(key []byte) (map[string][]byte, error) {
    hashData := make(map[string][]byte)
    options := rocks.NewDefaultReadOptions()
    defer options.Destroy()
    if obj, err := rh.loadRedisObject(options, key); err != nil {
        if err == ErrDoesNotExist {
            return hashData, nil
        }
        return nil, err
    } else {
        if obj.Type != kRedisHash {
            return nil, ErrWrongTypeRedisObject
        }
        data := obj.Data.([][]byte)
        for i := 0; i < len(data); i += 2 {
            hashData[string(data[i])] = data[i+1]
        }
        return hashData, nil
    }
}

const (
    kHashOpNone   = "noop"
    kHashOpSet    = "set"
    kHashOpDelete = "delete"
)

type HashOperand struct {
    Command string
    Key     string
    Value   []byte
}

func init() {
    gob.Register(&HashOperand{})
}

type HashMerger struct{}

func (m *HashMerger) FullMerge(existingObject *RedisObject, operands [][]byte) bool {
    rawData, ok := existingObject.Data.([][]byte)
    if !ok {
        rawData = [][]byte{}
    }
    hashData := make(map[string][]byte)
    for i := 0; i < len(rawData); i += 2 {
        hashData[string(rawData[i])] = rawData[i+1]
    }
    for _, operand := range operands {
        if obj, err := decode(operand, reflect.TypeOf(HashOperand{})); err == nil {
            op := obj.(HashOperand)
            switch op.Command {
            case kHashOpSet:
                hashData[op.Key] = op.Value
            case kHashOpDelete:
                delete(hashData, op.Key)
            }
        }
    }

    existingObject.Data = __hashToBytes(hashData)
    return true
}

func (m *HashMerger) PartialMerge(leftOperand, rightOperand []byte) ([]byte, bool) {
    obj, err := decode(leftOperand, reflect.TypeOf(HashOperand{}))
    if err != nil {
        return nil, false
    }
    leftOp := obj.(HashOperand)
    obj, err = decode(rightOperand, reflect.TypeOf(HashOperand{}))
    if err != nil {
        return nil, false
    }
    rightOp := obj.(HashOperand)
    if leftOp.Key == rightOp.Key {
        // fmt.Println("PartialMerged", leftOp, rightOp)
        return rightOperand, true
    }

    return nil, false
}

func __hashToBytes(hashData map[string][]byte) [][]byte {
    rawData := make([][]byte, len(hashData)*2)
    index := 0
    for key, value := range hashData {
        rawData[index] = []byte(key)
        rawData[index+1] = value
        index += 2
    }
    return rawData
}

var _ = fmt.Println
