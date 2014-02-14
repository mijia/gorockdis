package main

import (
    "log"
    "net"
    // "time"
    "fmt"
)

const (
    kDefaultAddress = ":6379"
)

type Server struct {
    Address string
}

func (s *Server) ListenAndServe() error {
    addr := s.Address
    if addr == "" {
        addr = kDefaultAddress
    }
    l, err := net.Listen("tcp", addr)
    if err != nil {
        return err
    }
    defer l.Close()
    log.Println("[ListenAndServe] GoRockdis is listening on", addr)

    for {
        conn, err := l.Accept()
        if err != nil {
            return err
        }
        go s.ServeClient(conn)
    }
}

func (s *Server) ServeClient(conn net.Conn) (err error) {
    clientAddr := conn.RemoteAddr().String()
    defer func() {
        if err != nil {
            log.Printf("[ServeClient] Client <%s> error in connection, will close it: %s", clientAddr, err)
            fmt.Fprintf(conn, "-ERROR %s\r\n", err)
        }
        conn.Close()
    }()

    return fmt.Errorf("This is a test")
}
