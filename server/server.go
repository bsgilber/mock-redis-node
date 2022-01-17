package main

import (
	"log"
	"net"
	"fmt"
	"time"
	// "os"
)

func main() {
	// Listen on TCP port 2000 on all available unicast and
	// anycast IP addresses of the local system.
	l, err := net.Listen("tcp", ":2000")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			defer c.Close()
			for {
				fmt.Printf("Connection from %s\n", c.RemoteAddr())

				n2, error := c.Write([]byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"))
				if error != nil {
					fmt.Printf("Cannot write: %s\n", error)
					break
				}
				fmt.Printf("Echoed %d bytes\n", n2)

				n3, error := c.Write([]byte("*4\r\n$5\r\nSETEX\r\n$1\r\na\r\n$1\r\n3\r\n$1\r\n1\r\n"))
				if error != nil {
					fmt.Printf("Cannot write: %s\n", error)
					break
				}
				fmt.Printf("Echoed %d bytes\n", n3)

				time.Sleep(2 * time.Second)
			}
		}(conn)
	}
}
