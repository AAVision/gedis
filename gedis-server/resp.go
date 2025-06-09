package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	SimpleStringPrefix = '+'
	ErrorPrefix        = '-'
	IntegerPrefix      = ':'
	BulkStringPrefix   = '$'
	ArrayPrefix        = '*'
)

func readArray(reader *bufio.Reader) ([]string, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, fmt.Errorf("expected '*', got '%c'", prefix)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")

	numElems, err := strconv.Atoi(line)
	if err != nil || numElems < 0 {
		return nil, errors.New("invalid array length")
	}

	args := make([]string, 0, numElems)
	for i := 0; i < numElems; i++ {
		arg, err := readBulkString(reader)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil

}

func readBulkString(reader *bufio.Reader) (string, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return "", err
	}
	if prefix != '$' {
		return "", fmt.Errorf("expected '$', got '%c'", prefix)
	}

	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")

	length, err := strconv.Atoi(line)
	if err != nil {
		return "", errors.New("invalid bulk string length")
	}
	if length < 0 {
		return "", nil
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return "", err
	}

	if _, err = reader.Discard(2); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeSimpleString(w io.Writer, s string) {
	w.Write([]byte("+" + s + "\r\n"))
}

func writeError(w io.Writer, msg string) {
	w.Write([]byte("-" + msg + "\r\n"))
}

func writeInteger(w io.Writer, n int64) {
	w.Write([]byte(":" + strconv.FormatInt(n, 10) + "\r\n"))
}

func writeBulkString(w io.Writer, s string) {
	if s == "" {
		w.Write([]byte("$-1\r\n"))
		return
	}
	w.Write([]byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"))
}

func writeNull(w io.Writer) {
	w.Write([]byte("$-1\r\n"))
}
