package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func handleConnection(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		args, err := readArray(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			writeError(writer, "ERR "+err.Error())
			writer.Flush()
			continue
		}

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		args = args[1:]

		switch cmd {
		case "PING":
			writeSimpleString(writer, "PONG")
		case "SET":
			if len(args) < 2 {
				writeError(writer, "ERR wrong number of arguments")
				break
			}
			store.Set(args[0], args[1])
			writeSimpleString(writer, "OK")
		case "GET":
			if len(args) != 1 {
				writeError(writer, "ERR wrong number of arguments")
				break
			}

			if value, exists := store.Get(args[0]); exists {
				writeBulkString(writer, fmt.Sprintf("%v", value))
			} else {
				writeNull(writer)
			}
		case "SETEX":
			if len(args) != 3 {
				writeError(writer, "ERR wrong number of arguments")
				break
			}
			secs, err := strconv.Atoi(args[1])
			if err != nil || secs <= 0 {
				writeError(writer, "ERR invalid expire time")
				break
			}
			store.SetEx(args[0], args[2], time.Duration(secs)*time.Second)
			writeSimpleString(writer, "OK")
		case "DEL":
			if len(args) < 1 {
				writeError(writer, "ERR wrong number of arguments")
				break
			}
			count := store.Del(args...)
			writeInteger(writer, int64(count))
		case "EXPIRE":
			if len(args) != 2 {
				writeError(writer, "ERR wrong number of arguments")
				break
			}
			secs, err := strconv.Atoi(args[1])
			if err != nil || secs <= 0 {
				writeError(writer, "ERR invalid expire time")
				break
			}
			if store.Expire(args[0], time.Duration(secs)*time.Second) {
				writeInteger(writer, 1)
			} else {
				writeInteger(writer, 0)
			}
		case "TTL":
			if len(args) != 1 {
				writeError(writer, "ERR wrong number of arguments")
				break
			}
			ttl := store.TTL(args[0])
			writeInteger(writer, int64(ttl.Seconds()))
		default:
			writeError(writer, "ERR unknown command")
		}

		writer.Flush()

	}
}

func main() {
	store := NewStore()
	defer store.Close()
	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatal("Failed to start server:", err)
	}

	defer listener.Close()
	log.Println("Gedis server listening on port 9999")

	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-shutdownCh
		log.Println("Shutting down server...")
		cancel()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				log.Println("Server stopped")
				return
			default:
				log.Println("Connection error:", err)
				continue
			}
		}
		go handleConnection(conn, store)
	}
}
