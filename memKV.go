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
	"strconv"
	"strings"
	"sync"
	"time"
)

/* Structure tp hold values and ttl */
type tVal struct {
	val string
	exp time.Time
}

/* The main kv  hash that holds all the keys and values */
var mainkv map[string]tVal
var kvLock = &sync.Mutex{}

/* Linked list to handle expiry of events */
type ttlExp struct {
	next  *ttlExp
	exp   time.Time
	lock  *sync.Mutex
	items map[string]bool
}

var expHash map[time.Time]ttlExp
var firstExpire *ttlExp
var chLock = &sync.RWMutex{}

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

func addExpiry(key string, ttl time.Time) error {

	chLock.RLock()
	t, found := expHash[ttl]
	chLock.RUnlock()
	if found {
		t.lock.Lock()
		t.items[key] = true
		t.lock.Unlock()
		return nil
	}
	chLock.Lock()
	defer chLock.Unlock()
	var ex ttlExp
	ex.exp = ttl
	ex.items[key] = true
	ex.next = nil
	expHash[ttl] = ex
	curr := firstExpire
	/* if this key is going to be the first to expire set it  */
	if firstExpire == nil || ttl.Before(firstExpire.exp) {
		firstExpire = &ex
		firstExpire.next = curr
		fmt.Printf("Inserting At the beginning\n")
		return nil
	}
	prev := firstExpire
	for {
		if curr.exp.Before(ttl) {
			prev.next = &ex
			ex.next = curr
			fmt.Printf("Inserting between %v and %v\n", prev.exp, curr.exp)
			return nil
		}
		if curr.next == nil {
			curr.next = &ex
			fmt.Printf("Inserting at the end \n")
			return nil
		}
		prev = curr
		curr = curr.next
	}
}
func setex(s []string) []byte {
	if len(s) != 4 {
		return []byte("ERROR: Need exactly 3 params for setex\n")
	}
	secs, err := strconv.Atoi(s[3])
	if err != nil {
		return []byte("ERROR: Invalid format for ttl in setex")
	}
	var t tVal
	t.val = s[2]
	t.exp = time.Now().Add(time.Second * time.Duration(secs))

	kvLock.Lock()
	mainkv[s[1]] = t
	kvLock.Unlock()
	addExpiry(t.val, t.exp)
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
	return []byte(mainkv[s[1]].val + "\n")
}
func delExpired() {
	for {

	}
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
	mainkv = make(map[string]tVal)
	expHash = make(map[time.Time]ttlExp)
	firstExpire = nil
	for {
		conn, _ := ln.Accept()
		go processConnection(conn)
	}

}
