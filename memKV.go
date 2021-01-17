package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func openSocket(addr string) net.Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("FATAL: Could not open socket %s \n", err)
		os.Exit(1)
	}
	return ln

}

func processConnection(conn net.Conn) {
	for {
		message, _ := bufio.NewReader(conn).ReadString('\n')
		if message == "exit\r\n" {
			conn.Write([]byte("Ok bye bye\n"))
			conn.Close()
			return
		}
		fmt.Printf("Message Received: <%s>\n", message)
	}
}

func main() {

	// open the socket for communication
	ln := openSocket(":9980")

	for {
		conn, _ := ln.Accept()
		go processConnection(conn)
	}

}
