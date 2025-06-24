package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
)

var (
	port = flag.Int("port", 50051, "the port to serve on")
)

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	addrs := []string{
		"localhost:50052",
		"localhost:50053",
	}

	i := 0
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Fatalf("failed to accept: %v", err)
		}
		go proxy(conn, addrs[i%len(addrs)])
		i++
	}
}

func proxy(ds net.Conn, addr string) {
	log.Printf("proxy conn: %s", addr)
	us, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("failed to dial %s: %v", addr, err)
		return
	}
	go io.Copy(us, ds)
	go io.Copy(ds, us)
}
