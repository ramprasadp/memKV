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
	exp int64
}

/* The main kv  hash that holds all the keys and values */
var mainkv map[string]tVal
var kvLock = &sync.Mutex{}

/* Linked list to handle expiry of events */
type ttlExp struct {
	next  *ttlExp
	name  string
	exp   int64
	lock  *sync.Mutex
	items map[string]bool
}

var expHash map[int64]ttlExp
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

func addExpiry(name string, ttl int64) error {

	fmt.Printf("%s => Looking for hash on epoch %d\n", name, ttl)
	chLock.RLock()
	t, found := expHash[ttl]
	chLock.RUnlock()
	if found {
		t.lock.Lock()
		t.items[name] = true
		t.lock.Unlock()
		return nil
	}
	chLock.Lock()
	defer chLock.Unlock()
	var ex ttlExp
	ex.exp = ttl
	ex.name = name
	ex.lock = &sync.Mutex{}
	ex.items = make(map[string]bool)
	ex.items[name] = true
	ex.next = nil
	expHash[ttl] = ex
	curr := firstExpire
	/* if this key is going to be the first to expire set it  */
	if firstExpire == nil || ttl < firstExpire.exp {
		firstExpire = &ex
		firstExpire.next = curr
		fmt.Printf("Inserting At the beginning\n")
		return nil
	}
	prev := firstExpire
	for {
		if curr.exp > ttl {
			prev.next = &ex
			ex.next = curr
			fmt.Printf("Inserting between %s and %s\n", prev.name, curr.name)
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
		return []byte("500 ERROR: Need exactly 3 params for setex\n")
	}
	secs, err := strconv.Atoi(s[3])
	if err != nil {
		return []byte("501 ERROR: Invalid format for ttl in setex")
	}
	var t tVal
	t.val = s[2]
	t.exp = time.Now().Unix() + int64(secs)
	fmt.Printf("key = %s , expiry = %v\n", s[1], t.exp)
	kvLock.Lock()
	mainkv[s[1]] = t
	kvLock.Unlock()
	err = addExpiry(s[1], t.exp)
	if err != nil {
		return []byte("502 ERROR: Error setting up Linked list")
	}

	return []byte("200 SUCCESS: set done\n")
}

func del(s []string) []byte {
	if len(s) != 2 {
		return []byte("ERROR: Need exactly 1 param for del\n")
	}

	t, found := mainkv[s[1]]
	if !found {
		return []byte("No such key\n")
	}
	ex, found := expHash[t.exp]
	if found {
		ex.lock.Lock()
		delete(ex.items, s[1])
		ex.lock.Unlock()
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
	fmt.Printf("Starting TTL thread for deleting the expired keys \n")
	for {
		curr := firstExpire
		if curr == nil {
			//	fmt.Printf("Nothing to expire waiting for some data\n")
			time.Sleep(5 * time.Second)
			continue
		}
		diff := curr.exp - time.Now().Unix()
		if diff > 0 {
			if diff > 20 {
				diff = 20
			}
			fmt.Printf("Sleeping for %d seconds\n", diff)
			time.Sleep(time.Duration(diff) * time.Second)
		}
		fmt.Printf("Expiring items for list name %s\n", curr.name)
		chLock.Lock()
		delete(expHash, curr.exp)
		firstExpire = curr.next
		chLock.Unlock()
		kvLock.Lock()
		for k := range curr.items {
			fmt.Printf("Expired item %s\n", k)
			delete(mainkv, k)
		}
		kvLock.Unlock()
		/* free() the memory of the list */
		curr = firstExpire
	}
}

func dump2(s []string) []byte {
	ret := "Start "
	curr := firstExpire
	for {
		if curr == nil {
			return []byte(ret + "\n")
		}
		items := ""
		for k := range curr.items {
			items = items + " " + k

		}
		ret = fmt.Sprintf(" %s -> %s (%s)  ", ret, curr.name, items)
		curr = curr.next
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
		case "_dump2":
			conn.Write(dump2(s))
		default:
			conn.Write([]byte("ERROR: Not supported \n"))
		}
		fmt.Printf("Message Received: <%s>\n", message)
	}
}

func main() {

	// open the socket for communication
	ln := openSocket("0.0.0.0:9980")
	mainkv = make(map[string]tVal)
	expHash = make(map[int64]ttlExp)
	firstExpire = nil
	go delExpired()
	for {
		conn, _ := ln.Accept()
		go processConnection(conn)
	}

}
