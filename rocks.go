package main

type RocksDBHandler struct {
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
