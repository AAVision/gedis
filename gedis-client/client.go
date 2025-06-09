package gedis_client

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrInvalidType = errors.New("invalid type")
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) doCommand(args ...string) (interface{}, error) {
	c.writer.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")

	for _, arg := range args {
		c.writer.WriteString("$" + strconv.Itoa(len(arg)) + "\r\n")
		c.writer.WriteString(arg + "\r\n")
	}

	if err := c.writer.Flush(); err != nil {
		return nil, err
	}

	resp, err := c.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch resp {
	case '+':
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return line[:len(line)-2], nil
	case '-':
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return nil, errors.New(line[:len(line)-2])
	case ':':
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return strconv.ParseInt(line[:len(line)-2], 10, 64)
	case '$':
		line, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		length, _ := strconv.Atoi(line[:len(line)-2])
		if length < 0 {
			return nil, nil
		}
		buf := make([]byte, length)
		_, err = io.ReadFull(c.reader, buf)
		if err != nil {
			return nil, err
		}

		if _, err = c.reader.Discard(2); err != nil {
			return nil, err
		}
		return string(buf), nil
	default:
		return nil, fmt.Errorf("unexpected response prefix: %c", resp)
	}
}

func (c *Client) Ping() (string, error) {
	resp, err := c.doCommand("PING")
	if err != nil {
		return "", err
	}
	return resp.(string), nil
}

func (c *Client) Set(key, value string) error {
	_, err := c.doCommand("SET", key, value)
	return err
}

func (c *Client) SetEx(key, value string, ttl time.Duration) error {
	secs := int(ttl.Seconds())
	_, err := c.doCommand("SET", key, value, "EX", strconv.Itoa(secs))
	return err
}

func (c *Client) Get(key string) (interface{}, error) {
	resp, err := c.doCommand("GET", key)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, ErrKeyNotFound
	}
	return resp, nil
}

func (c *Client) GetString(key string) (string, error) {
	val, err := c.Get(key)
	if err != nil {
		return "", err
	}
	str, ok := val.(string)
	if !ok {
		return "", ErrInvalidType
	}
	return str, nil
}

func (c *Client) GetInt(key string) (int, error) {
	val, err := c.Get(key)
	if err != nil {
		return 0, err
	}
	str, ok := val.(string)
	if !ok {
		return 0, ErrInvalidType
	}

	return strconv.Atoi(str)
}

func (c *Client) GetFloat(key string) (float64, error) {
	val, err := c.Get(key)
	if err != nil {
		return 0, err
	}
	str, ok := val.(string)
	if !ok {
		return 0, ErrInvalidType
	}
	return strconv.ParseFloat(str, 64)
}

func (c *Client) Del(keys ...string) (int, error) {
	args := append([]string{"DEL"}, keys...)
	resp, err := c.doCommand(args...)
	if err != nil {
		return 0, err
	}
	return int(resp.(int64)), nil
}

func (c *Client) Expire(key string, ttl time.Duration) (bool, error) {
	secs := int(ttl.Seconds())
	resp, err := c.doCommand("EXPIRE", key, strconv.Itoa(secs))
	if err != nil {
		return false, err
	}
	return resp.(int64) == 1, nil
}

func (c *Client) TTL(key string) (time.Duration, error) {
	resp, err := c.doCommand("TTL", key)
	if err != nil {
		return 0, err
	}
	secs := resp.(int64)
	return time.Duration(secs) * time.Second, nil
}

func (c *Client) Keys() ([]string, error) {
	keys, err := c.doCommand("KEYS")
	if err != nil {
		return []string{""}, err
	}
	result := make([]string, len(keys.([]string)))
	for i, key := range keys.([]string) {
		result[i] = fmt.Sprintf("%d) \"%s\"", i+1, key)
	}
	return result, nil
}

func (c *Client) FlushDB() (string, error) {
	_, err := c.doCommand("FLUSHDB")
	if err != nil {
		return "", err
	}
	return "OK", nil
}
