package main

import (
    "encoding/gob"
    "fmt"
    rocks "github.com/tecbot/gorocksdb"
    "reflect"
    "strconv"
)

func (rh *RocksDBHandler) RedisAppend(key, value []byte) (int, error) {
    if err := rh.checkRedisCall(key, value); err != nil {
        return 0, err
    }
    if err := rh.checkKeyType(key, kRedisString); err != nil {
        return 0, err
    }

    if err := doStringMergeOperation(rh.db, rh.getTypeKey(key), key, value, kStringOpAppend); err != nil {
        return 0, err
    }
    if data, err := rh.RedisGet(key); err == nil {
        return len(data), nil
    } else {
        return 0, err
    }
}

func (rh *RocksDBHandler) RedisDecr(key []byte) ([]byte, error) {
    return rh.RedisIncrBy(key, -1)
}

func (rh *RocksDBHandler) RedisDecrBy(key []byte, value int) ([]byte, error) {
    return rh.RedisIncrBy(key, -1*value)
}

func (rh *RocksDBHandler) RedisIncr(key []byte) ([]byte, error) {
    return rh.RedisIncrBy(key, 1)
}

func (rh *RocksDBHandler) RedisIncrBy(key []byte, value int) ([]byte, error) {
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }
    if err := rh.checkKeyType(key, kRedisString); err != nil {
        return nil, err
    }

    data := []byte(strconv.Itoa(value))
    if err := doStringMergeOperation(rh.db, rh.getTypeKey(key), key, data, kStringOpIncr); err != nil {
        return nil, err
    }
    return rh.RedisGet(key)
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
    if err := rh.checkRedisCall(key); err != nil {
        return nil, err
    }

    options := rocks.NewDefaultReadOptions()
    defer options.Destroy()
    if obj, err := rh.loadRedisObject(options, key); err != nil {
        if err == ErrDoesNotExist {
            return []byte{}, nil
        }
        return nil, err
    } else {
        if obj.Type != kRedisString {
            return nil, ErrWrongTypeRedisObject
        }
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
            if obj.Type == kRedisString {
                results[i] = obj.Data.([]byte)
            }
        }
    }
    return results, nil
}

func (rh *RocksDBHandler) RedisSet(key, value []byte) error {
    if err := rh.checkRedisCall(key, value); err != nil {
        return err
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

func doStringMergeOperation(db *rocks.DB, typeKey, key, value []byte, opCode string) error {
    options := rocks.NewDefaultWriteOptions()
    defer options.Destroy()
    batch := rocks.NewWriteBatch()
    defer batch.Destroy()
    batch.Put(typeKey, []byte(kRedisString))
    operand := StringOperand{opCode, value}
    if data, err := encode(operand); err != nil {
        return err
    } else {
        batch.Merge(key, data)
    }
    if err := db.Write(options, batch); err != nil {
        return err
    }
    return nil
}

const (
    kStringOpIncr   = "incr"
    kStringOpAppend = "append"
)

type StringOperand struct {
    Command string
    Data    []byte
}

func init() {
    gob.Register(&StringOperand{})
}

type StringMerger struct {
}

func (m *StringMerger) FullMerge(existingObject *RedisObject, operands [][]byte) bool {
    appendData, ok := existingObject.Data.([]byte)
    if !ok {
        return false
    }
    incrData, err := strconv.ParseInt(string(appendData), 10, 64)
    if err != nil {
        incrData = 0
    }

    lastOp := ""
    for _, operand := range operands {
        if op, err := decode(operand, reflect.TypeOf(StringOperand{})); err == nil {
            lastOp = op.(StringOperand).Command
            switch lastOp {
            case kStringOpIncr:
                if n, err := strconv.ParseInt(string(op.(StringOperand).Data), 10, 64); err == nil {
                    incrData += n
                    appendData = []byte(fmt.Sprintf("%d", incrData))
                }
            case kStringOpAppend:
                appendData = append(appendData, op.(StringOperand).Data...)
                if n, err := strconv.ParseInt(string(appendData), 10, 64); err == nil {
                    incrData = n
                } else {
                    incrData = 0
                }
            }
        }
    }
    switch lastOp {
    case kStringOpIncr:
        existingObject.Data = []byte(fmt.Sprintf("%d", incrData))
        return true
    case kStringOpAppend:
        existingObject.Data = appendData
        return true
    }
    return false
}

func (m *StringMerger) PartialMerge(leftOperand, rightOperand []byte) ([]byte, bool) {
    obj, err := decode(leftOperand, reflect.TypeOf(StringOperand{}))
    if err != nil {
        return nil, false
    }
    leftOp := obj.(StringOperand)
    obj, err = decode(rightOperand, reflect.TypeOf(StringOperand{}))
    if err != nil {
        return nil, false
    }
    rightOp := obj.(StringOperand)
    if leftOp.Command == rightOp.Command {
        mergeOp := StringOperand{Command: leftOp.Command}
        merged := false
        switch leftOp.Command {
        case kStringOpIncr:
            left, leftErr := strconv.ParseInt(string(leftOp.Data), 10, 64)
            right, rightErr := strconv.ParseInt(string(rightOp.Data), 10, 64)
            if leftErr == nil && rightErr == nil {
                mergeOp.Data, merged = []byte(fmt.Sprintf("%d", left+right)), true
            } else if leftErr == nil {
                mergeOp.Data, merged = []byte(fmt.Sprintf("%d", left)), true
            } else if rightErr == nil {
                mergeOp.Data, merged = []byte(fmt.Sprintf("%d", right)), true
            }
        case kStringOpAppend:
            mergeOp.Data, merged = append(leftOp.Data, rightOp.Data...), true
        }
        if merged {
            if data, err := encode(mergeOp); err == nil {
                return data, true
            }
        }
    }
    return nil, false
}
