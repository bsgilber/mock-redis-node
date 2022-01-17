package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	// "time"
)

func Filter(vs []string, f func(string) bool) []string {
	vsf := make([]string, 0)
	for _, v := range vs {
		if f(v) {
			vsf = append(vsf, v)
		}
	}
	return vsf
}

func main() {
	// request connection to the server over tcp
	conn, err := net.Dial("tcp", "[::]:6379")

	if err != nil {
		fmt.Print("dial failed\n")
	}

	// inform the server you are a replica server
	// SYNC for full history, PSYNC to start at a specific offset
	c := bufio.NewReader(conn)
	conn.Write([]byte("INFO replication\n"))

	var masterId string
	var lastOffset int

	for {
		raw, _, err := c.ReadLine()
		if err != nil || len(raw) == 0 {
			break
		}

		msg := string(raw)
		fmt.Printf("%s\n", msg)
		if strings.Contains(msg, "master_replid:") {
			masterId = strings.Split(msg, ":")[1]
		}
		if strings.Contains(msg, "master_repl_offset:") {
			lastOffset, _ = strconv.Atoi(strings.Split(msg, ":")[1])
		}
	}

	// localAddr := strings.Split(conn.LocalAddr().String(), ":")
	// port := localAddr[len(localAddr)-1]

	conn.Write([]byte(fmt.Sprint("PING\r\n")))
	conn.Write([]byte(fmt.Sprintf("REPLCONF listening-port %d\r\n", 1532)))
	// conn.Write([]byte(fmt.Sprintf("REPLCONF ip-address %s\r\n", "127.0.0.1")))
	conn.Write([]byte(fmt.Sprint("REPLCONF capa eof\r\n")))
	conn.Write([]byte(fmt.Sprintf("PSYNC %s %d\r\n", masterId, lastOffset)))

	fmt.Printf("%s -- %d\n", masterId, lastOffset)

	for {
		// each new redis event protocol begins with '*'
		// slice byte array on byte of char '*'
		// rest of array remains in buffer
		// rawMsg, _ := c.ReadBytes(byte('\r'))
		rawMsg, err := c.ReadBytes(byte('*'))
		if len(rawMsg) == 0 || err != nil {
			// c = bufio.NewReader(conn)
			// conn.Write([]byte(fmt.Sprintf("PSYNC %s %d\n", masterId, lastOffset)))
			fmt.Print("buffer empty\n")
			time.Sleep(1 * time.Second)
			continue
		}
		if strings.Contains(string(rawMsg), "PING") {
			conn.Write([]byte(fmt.Sprint("PING\r\n")))
			conn.Write([]byte(fmt.Sprintf("REPLCONF ACK %d\r\n", lastOffset)))
		}

		// if err != nil {
		// 	fmt.Printf("error %s", err)
		// 	continue
		// }

		// turn the byte array into a string array
		// split string on "\r\n" (CRLF); see redis protocol
		// '*' is not dropped from ReadBytes, dropped with [1:]
		msg := strings.Split(string(rawMsg[:len(rawMsg)-1]), "\r\n")

		// statements of the form \$\d+ specify the number of
		// bytes in the next statement; skip these
		cmdString := Filter(msg, func(v string) bool {
			return !strings.HasPrefix(v, "$")
		})

		if len(cmdString) > 0 {
			fmt.Printf("readfull resp: [%s]\n", strings.Join(cmdString, " "))
		}

		fmt.Printf("lastOffset: %d\n", lastOffset)
		lastOffset++
	}
}

/*
	For Simple Strings the first byte of the reply is "+"
	For Errors the first byte of the reply is "-"
	For Integers the first byte of the reply is ":"
	For Bulk Strings the first byte of the reply is "$"
	For Arrays the first byte of the reply is "*"
*/
