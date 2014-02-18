package main

// Server and Connection Command
func (rh *RocksDBHandler) RedisSelect(db int) error {
    if rh.db == nil {
        return ErrRocksIsDead
    }
    return nil
}

func (rh *RocksDBHandler) RedisPing() (*StatusReply, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    return &StatusReply{"PONG"}, nil
}

func (rh *RocksDBHandler) RedisInfo() ([]byte, error) {
    if rh.db == nil {
        return nil, ErrRocksIsDead
    }
    return []byte("TBD\r\n"), nil
}
