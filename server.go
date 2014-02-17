package main

import (
    "bytes"
    "fmt"
    "io"
    "log"
    "net"
    "reflect"
    "strings"
    "time"
)

const (
    kDefaultAddress = ":6379"
)

type HandlerFn func(request *Request) (Reply, error)
type GuarderFn func(request *Request) (reflect.Value, *ErrorReply)

type Server struct {
    Address    string
    Methods    map[string]HandlerFn
    MonitorLog bool
}

func (s *Server) RegisterHandler(handler interface{}) error {
    hType := reflect.TypeOf(handler)
    for i := 0; i < hType.NumMethod(); i++ {
        method := hType.Method(i)
        if method.Name[0] >= 'a' && method.Name[0] <= 'z' {
            continue
        }
        hFn, err := s.newHandler(handler, &method.Func)
        if err != nil {
            return err
        }
        s.Methods[strings.ToLower(method.Name)] = hFn
    }
    return nil
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
            log.Printf("[ServeClient] Error in request/reply, will close the connnetion <%s>: %s", clientAddr, err)
            fmt.Fprintf(conn, "-ERROR %s\r\n", err)
        }
        conn.Close()
        conn = nil
    }()

    for {
        conn.SetReadDeadline(time.Now())
        zeroByte := make([]byte, 0)
        if _, err := conn.Read(zeroByte); err == io.EOF {
            log.Printf("[ServeClient] Detect a closed connection on %s", clientAddr)
            break
        }

        conn.SetReadDeadline(time.Now().Add(10 * time.Minute))
        request, err := NewRequest(conn)
        if err == io.EOF {
            log.Printf("[ServeClient] Detect a closed connection on %s", clientAddr)
            break
        } else {
            if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
                log.Printf("[ServeClient] Detect a Timeout client on %s, will check if she is closed.", clientAddr)
            } else {
                if err != nil {
                    return err
                }
            }
        }
        request.RemoteAddress = clientAddr
        if reply, err := s.ServeRequest(request); err != nil {
            return err
        } else {
            if _, err := reply.WriteTo(conn); err != nil {
                return err
            }
        }
    }
    return nil
}

func (s *Server) ServeRequest(request *Request) (Reply, error) {
    if fn, ok := s.Methods[strings.ToLower(request.Command)]; ok {
        return fn(request)
    } else {
        return ErrMethodNotSupported, nil
    }
}

func (s *Server) Close() {
    log.Printf("[Server] Server Stopped.")
}

func NewServer(config Config) *Server {
    s := &Server{}
    s.Methods = make(map[string]HandlerFn)
    s.Address = fmt.Sprintf("%s:%d", config.Server.Bind, config.Server.Port)
    s.MonitorLog = config.Server.MonitorLog
    return s
}

func (s *Server) newHandler(handler interface{}, f *reflect.Value) (HandlerFn, error) {
    errType := reflect.TypeOf(s.newHandler).Out(1) // get the error's type
    guards, err := s.newHandlerGuards(handler, f)
    if err != nil {
        return nil, err
    }

    fType := f.Type()
    if fType.NumOut() == 0 {
        return nil, fmt.Errorf("Not enough return values")
    }
    if fType.NumOut() > 2 {
        return nil, fmt.Errorf("Too many return values")
    }
    if t := fType.Out(fType.NumOut() - 1); t != errType {
        return nil, fmt.Errorf("Last return value must be an error type (not %s)", t)
    }

    return s.newHandlerFn(handler, f, guards), nil
}

func (s *Server) newHandlerGuards(handler interface{}, f *reflect.Value) ([]GuarderFn, error) {
    guards := []GuarderFn{}
    fType := f.Type()

    inputStart := 0
    if fType.NumIn() > 0 && fType.In(0).AssignableTo(reflect.TypeOf(handler)) {
        // In: [*handlerType, arg0, arg1 ...]
        inputStart = 1
    }
    for i := inputStart; i < fType.NumIn(); i++ {
        switch fType.In(i) {
        case reflect.TypeOf([]byte{}):
            guards = append(guards, guardRequestByteArg(i-inputStart))
        case reflect.TypeOf([][]byte{}):
            guards = append(guards, guardRequestByteSliceArg(i-inputStart))
        case reflect.TypeOf(1):
            guards = append(guards, guardRequestIntArg(i-inputStart))
        default:
            return nil, fmt.Errorf("Argument %d: wrong type %s (%s)", i, fType.In(i), fType.Name())
        }
    }
    return guards, nil
}

func (s *Server) newHandlerFn(handler interface{}, f *reflect.Value, guards []GuarderFn) HandlerFn {
    return func(request *Request) (Reply, error) {
        input := []reflect.Value{reflect.ValueOf(handler)}
        for _, guard := range guards {
            value, errReply := guard(request)
            if errReply != nil {
                return errReply, nil
            }
            input = append(input, value)
        }
        if f.Type().NumIn() == 0 {
            input = []reflect.Value{}
        } else if !f.Type().In(0).AssignableTo(reflect.TypeOf(handler)) {
            input = input[1:]
        }

        var monitorString string
        if len(request.Arguments) > 0 {
            monitorString = fmt.Sprintf("%.6f [0 %s] \"%s\" \"%s\"",
                float64(time.Now().UTC().UnixNano())/1e9,
                request.RemoteAddress,
                request.Command,
                bytes.Join(request.Arguments, []byte{'"', ' ', '"'}))
        } else {
            monitorString = fmt.Sprintf("%.6f [0 %s] \"%s\"",
                float64(time.Now().UTC().UnixNano())/1e9,
                request.RemoteAddress,
                request.Command)
        }
        if s.MonitorLog {
            log.Printf("[Monitor] %s", monitorString)
        }

        var results []reflect.Value
        if f.Type().IsVariadic() {
            results = f.CallSlice(input)
        } else {
            results = f.Call(input)
        }
        if err := results[len(results)-1].Interface(); err != nil {
            return &ErrorReply{err.(error).Error()}, nil
        }
        if len(results) > 1 {
            return NewReply(s, request, results[0].Interface())
        }
        return &StatusReply{"OK"}, nil
    }
}
