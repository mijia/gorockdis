Wrap RocksDB inside a server talks like the REDIS.

Dependencies:
* RocksDB
* RocksDB Go Wrapper: go get github.com/mijia/gorocks
* go get code.google.com/p/gcfg

Support commands:
GET, SET, MGET, DEL, INFO

Config:
Please refer to rockdis.conf for example.
$ go run *.go -conf=rockdis.conf

Thanks for the idea and code from https://github.com/dotcloud/go-redis-server.