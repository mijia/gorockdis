package main

import (
    "bufio"
    "fmt"
    "io"
    "io/ioutil"
    "reflect"
    "strconv"
    "strings"
)

type Request struct {
    Command       string
    Arguments     [][]byte
    RemoteAddress string
    Connection    io.ReadCloser
}

func (r *Request) HasArgument(index int) bool {
    return index >= 0 && index < len(r.Arguments)
}

func (r *Request) ExpectArgument(index int) *ErrorReply {
    if !r.HasArgument(index) {
        return ErrNotEnoughArgs
    }
    return nil
}

func (r *Request) GetInt(index int) (int, *ErrorReply) {
    if errReply := r.ExpectArgument(index); errReply != nil {
        return -1, errReply
    }
    if n, err := strconv.Atoi(string(r.Arguments[index])); err != nil {
        return -1, ErrExpectInteger
    } else {
        return n, nil
    }
}

func NewRequest(conn io.ReadCloser) (*Request, error) {
    reader := bufio.NewReader(conn)

    // *<number of arguments>CRLF
    line, err := reader.ReadString('\n')
    if err != nil {
        return nil, err
    }

    var argCount int
    if line[0] == '*' {
        if _, err := fmt.Sscanf(line, "*%d\r", &argCount); err != nil {
            return nil, Malformed("*<#Arguments>", line)
        }
        // $<number of bytes of argument 1>CRLF
        // <argument data>CRLF
        command, err := readArgument(reader)
        if err != nil {
            return nil, err
        }
        arguments := make([][]byte, argCount-1)
        for i := 0; i < argCount-1; i++ {
            if arguments[i], err = readArgument(reader); err != nil {
                return nil, err
            }
        }
        return &Request{
            Command:    strings.ToLower(string(command)),
            Arguments:  arguments,
            Connection: conn,
        }, nil
    }

    // Inline request:
    fields := strings.Split(strings.Trim(line, "\r\n"), " ")
    var arguments [][]byte
    if len(fields) > 1 {
        for _, arg := range fields[1:] {
            arguments = append(arguments, []byte(arg))
        }
    }
    return &Request{
        Command:    strings.ToLower(fields[0]),
        Arguments:  arguments,
        Connection: conn,
    }, nil
}

func readArgument(reader *bufio.Reader) ([]byte, error) {
    line, err := reader.ReadString('\n')
    if err != nil {
        return nil, Malformed("$<ArgumentLength>", line)
    }

    var argLength int
    if _, err := fmt.Sscanf(line, "$%d\r", &argLength); err != nil {
        return nil, Malformed("$<ArgumentLength>", line)
    }

    data, err := ioutil.ReadAll(io.LimitReader(reader, int64(argLength)))
    if err != nil {
        return nil, err
    }
    if len(data) != argLength {
        return nil, MalformedLength(argLength, len(data))
    }
    if b, err := reader.ReadByte(); err != nil || b != '\r' {
        return nil, MalformedMissingCRLF()
    }
    if b, err := reader.ReadByte(); err != nil || b != '\n' {
        return nil, MalformedMissingCRLF()
    }

    return data, nil
}

func Malformed(expected string, got string) error {
    return fmt.Errorf("Mailformed request: %s does not match %s", got, expected)
}

func MalformedLength(expected int, got int) error {
    return fmt.Errorf("Mailformed request: argument length %d does not match %d", got, expected)
}

func MalformedMissingCRLF() error {
    return fmt.Errorf("Mailformed request: line should end with CRLF")
}

type Reply io.WriterTo

var (
    ErrMethodNotSupported   = &ErrorReply{"Method is not supported"}
    ErrNotEnoughArgs        = &ErrorReply{"Not enough arguments for the command"}
    ErrTooMuchArgs          = &ErrorReply{"Too many arguments for the command"}
    ErrWrongArgsNumber      = &ErrorReply{"Wrong number of arguments"}
    ErrExpectInteger        = &ErrorReply{"Expected integer"}
    ErrExpectPositivInteger = &ErrorReply{"Expected positive integer"}
    ErrExpectMorePair       = &ErrorReply{"Expected at least one key val pair"}
    ErrExpectEvenPair       = &ErrorReply{"Got uneven number of key val pairs"}
)

type ErrorReply struct {
    message string
}

func (er *ErrorReply) WriteTo(w io.Writer) (int64, error) {
    n, err := w.Write([]byte("-ERROR " + er.message + "\r\n"))
    return int64(n), err
}

func (er *ErrorReply) Error() string {
    return er.message
}

func guardRequestByteArg(index int) GuarderFn {
    return func(request *Request) (reflect.Value, *ErrorReply) {
        if err := request.ExpectArgument(index); err != nil {
            return reflect.ValueOf([]byte{}), nil
        } else {
            return reflect.ValueOf(request.Arguments[index]), nil
        }
    }
}

func guardRequestByteSliceArg(index int) GuarderFn {
    return func(request *Request) (reflect.Value, *ErrorReply) {
        if err := request.ExpectArgument(index); err != nil {
            return reflect.ValueOf([][]byte{}), nil
        } else {
            return reflect.ValueOf(request.Arguments[index:]), nil
        }
    }
}

func guardRequestIntArg(index int) GuarderFn {
    return func(request *Request) (reflect.Value, *ErrorReply) {
        n, err := request.GetInt(index)
        return reflect.ValueOf(n), err
    }
}

type StatusReply struct {
    code string
}

func (r *StatusReply) WriteTo(w io.Writer) (int64, error) {
    n, err := w.Write([]byte("+" + r.code + "\r\n"))
    return int64(n), err
}

type IntReply struct {
    number int
}

func (r *IntReply) WriteTo(w io.Writer) (int64, error) {
    n, err := w.Write([]byte(":" + strconv.Itoa(r.number) + "\r\n"))
    return int64(n), err
}

type BulkReply struct {
    value []byte
}

func (r *BulkReply) WriteTo(w io.Writer) (int64, error) {
    return writeBytes(r.value, w)
}

type MultiBulkReply struct {
    values [][]byte
}

func (r *MultiBulkReply) WriteTo(w io.Writer) (int64, error) {
    if r.values == nil {
        return 0, fmt.Errorf("Multi bulk reply found a nil values")
    }
    if wrote, err := w.Write([]byte("*" + strconv.Itoa(len(r.values)) + "\r\n")); err != nil {
        return int64(wrote), err
    } else {
        total := int64(wrote)
        for _, value := range r.values {
            wroteData, err := writeBytes(value, w)
            total += wroteData
            if err != nil {
                return total, err
            }
        }
        return total, nil
    }
}

func NewReply(s *Server, request *Request, value interface{}) (Reply, error) {
    switch v := value.(type) {
    case []byte:
        return &BulkReply{v}, nil
    case [][]byte:
        return &MultiBulkReply{v}, nil
    case int:
        return &IntReply{v}, nil
    case *StatusReply:
        return v, nil
    default:
        return nil, fmt.Errorf("Unsupported type: %s (%T)", v, v)
    }
}

func writeNullBytes(w io.Writer) (int64, error) {
    n, err := w.Write([]byte("$-1\r\n"))
    return int64(n), err
}

func writeBytes(value interface{}, w io.Writer) (int64, error) {
    if value == nil {
        return writeNullBytes(w)
    }
    switch v := value.(type) {
    case []byte:
        if len(v) == 0 {
            return writeNullBytes(w)
        }
        if wrote, err := w.Write([]byte("$" + strconv.Itoa(len(v)) + "\r\n")); err != nil {
            return int64(wrote), err
        } else if wroteData, err := w.Write([]byte(v)); err != nil {
            return int64(wrote + wroteData), err
        } else {
            wroteCRLF, err := w.Write([]byte("\r\n"))
            return int64(wrote + wroteData + wroteCRLF), err
        }
    case int:
        wrote, err := w.Write([]byte(":" + strconv.Itoa(v) + "\r\n"))
        return int64(wrote), err
    }
    return 0, fmt.Errorf("Invalid type sent to WriteBytes")
}
