package main

import (
    "fmt"
    rocks "github.com/tecbot/gorocksdb"
    "runtime"
    "sync"
)

type Handler struct{}

func (h *Handler) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
    fmt.Println("FullMerge called.")
    for _, operand := range operands {
        existingValue = append(existingValue, operand...)
    }
    return existingValue, true
}

func (h *Handler) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
    return nil, false
}

func (h *Handler) Name() string {
    return "GoRockdisMergeOperator"
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    opts := rocks.NewDefaultOptions()
    opts.SetCreateIfMissing(true)
    opts.SetMergeOperator(rocks.NewMergeOperator(&Handler{}))

    db, err := rocks.OpenDb(opts, "/opt/tmp/rocksdb")
    if err != nil {
        panic(err)
    }

    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        ro := rocks.NewDefaultReadOptions()
        wo := rocks.NewDefaultWriteOptions()

        err = db.Merge(wo, []byte("test"), []byte("hello"))
        if err != nil {
            panic(err)
        }

        data, err := db.Get(ro, []byte("test"))
        if err != nil {
            panic(err)
        }
        fmt.Println(string(data.Data()))
        wg.Done()
    }()

    wg.Wait()
}
