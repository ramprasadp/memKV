package main

/*
 Simple KV in memory. Supports foll commands in lowercase : quit get set del delmany(csv string) _dump (Debug only)
Only 3 important config params

port number
redis server
redis queue name

*/

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

/* The main kv  hash that holds all the keys and values */
var mainkv map[string]string
var kvLock = &sync.Mutex{}

func openSocket(addr string) net.Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("FATAL: Could not open socket %s \n", err)
		os.Exit(1)
	}
	return ln

}

func dump(s []string) []byte {
	ret := fmt.Sprintf("%v\n\n", mainkv)
	return []byte(ret)

}

func setex(s []string) []byte {
	if len(s) != 4 {
		return []byte("ERROR: Need exactly 3 params for setex\n")
	}
	kvLock.Lock()
	mainkv[s[1]] = s[2]
	kvLock.Unlock()

	return []byte("set done\n")
}

func del(s []string) []byte {
	if len(s) != 2 {
		return []byte("ERROR: Need exactly 1 param for del\n")
	}
	kvLock.Lock()
	delete(mainkv, s[1])
	kvLock.Unlock()
	return []byte("Del complete\n")
}

func get(s []string) []byte {
	if len(s) != 2 {
		return []byte("ERROR: Need exactly 1 param for get\n")
	}
	kvLock.Lock()
	defer kvLock.Unlock()
	return []byte(mainkv[s[1]] + "\n")
}
func processConnection(conn net.Conn) {
	for {
		message, _ := bufio.NewReader(conn).ReadString('\n')
		s := strings.Fields(message)
		if len(s) < 1 {
			continue
		}
		switch s[0] {
		case "quit":
			conn.Write([]byte("Ok bye bye\n"))
			conn.Close()
			return
		case "setex":
			conn.Write(setex(s))
		case "get":
			conn.Write(get(s))
		case "del":
			conn.Write(del(s))
		case "_dump":
			conn.Write(dump(s))
		default:
			conn.Write([]byte("ERROR: Not supported \n"))
		}
		fmt.Printf("Message Received: <%s>\n", message)
	}
}

func main() {

	// open the socket for communication
	ln := openSocket(":9980")
	mainkv = make(map[string]string)
	for {
		conn, _ := ln.Accept()
		go processConnection(conn)
	}

}
