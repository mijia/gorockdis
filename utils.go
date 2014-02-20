package main

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "log"
    "reflect"
    "strconv"
    "strings"
    "sync/atomic"
)

func encode(value interface{}) ([]byte, error) {
    buffer := new(bytes.Buffer)
    encoder := gob.NewEncoder(buffer)
    if err := encoder.Encode(value); err != nil {
        log.Printf("[Encode] Error when encode object, %s", err)
        return nil, err
    }
    return buffer.Bytes(), nil
}

func decode(data []byte, vType reflect.Type) (interface{}, error) {
    v := reflect.New(vType)
    buffer := bytes.NewBuffer(data)
    decoder := gob.NewDecoder(buffer)
    if err := decoder.DecodeValue(v); err != nil {
        log.Printf("[Decode] Error when decode object, %s", err)
        return nil, err
    }
    return reflect.Indirect(v).Interface(), nil
}

func parseComputerSize(size string) (int, error) {
    oneKBytes := 1 << 10
    oneMBytes := 1 << 20
    oneGBytes := 1 << 30
    var (
        count int
        bits  byte
    )
    if _, err := fmt.Sscanf(strings.ToLower(size), "%d%c", &count, &bits); err != nil {
        return 0, err
    }
    switch bits {
    case 'k':
        return count * oneKBytes, nil
    case 'm':
        return count * oneMBytes, nil
    case 'g':
        return count * oneGBytes, nil
    }
    return 0, fmt.Errorf("[Config] Format error")
}

type AtomicInt int64

func (i *AtomicInt) Add(n int64) {
    atomic.AddInt64((*int64)(i), n)
}

func (i *AtomicInt) Set(n int64) {
    atomic.StoreInt64((*int64)(i), n)
}

func (i *AtomicInt) Get() int64 {
    return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
    return strconv.FormatInt(i.Get(), 10)
}
