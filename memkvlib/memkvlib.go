package memkvlib

import (
	"bytes"
	"fmt"
	"net"
)

// Memdbh struct to hold the memkv connection
type Memdbh struct {
	conn net.Conn
	addr string
}

//SUCCESS byte array for 200
var SUCCESS []byte = []byte("200")

// Connect to memKV
func Connect(addr string) (*Memdbh, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	var m Memdbh
	m.conn = c
	m.addr = addr
	return &m, nil
}

//Setex sets the key val with ttl
func (M *Memdbh) Setex(key string, val string, ttl int) ([]byte, error) {
	cmd := []byte(fmt.Sprintf("setex %s %s %d\r\n", key, val, ttl))
	_, err := M.conn.Write(cmd)
	if err != nil {
		return nil, err
	}
	reply := make([]byte, 1024)
	_, err = M.conn.Read(reply)
	if bytes.HasPrefix(reply, SUCCESS) {
		return reply, nil
	}
	return nil, fmt.Errorf("Unexpected reply %s %s ", reply, err)
}
