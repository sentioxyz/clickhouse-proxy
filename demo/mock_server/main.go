package main

import (
	"flag"
	"log"
	"net"

	"github.com/ClickHouse/ch-go/proto"
)

func main() {
	addr := flag.String("addr", ":19000", "listen address")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	log.Printf("Mock server listening on %s", *addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept:", err)
			continue
		}
		go handle(conn)
	}
}

func handle(conn net.Conn) {
	defer conn.Close()

	r := proto.NewReader(conn)
	// Read packet type (should be 0 for Hello)
	pType, err := r.UVarInt()
	if err != nil {
		log.Println("read packet type:", err)
		return
	}
	if pType != 0 { // Hello
		log.Printf("unexpected packet type: %d", pType)
		return
	}

	var clientHello proto.ClientHello
	if err := clientHello.Decode(r); err != nil {
		log.Println("decode client hello:", err)
		return
	}
	// Critical for verification: Log the user and password received!
	log.Printf("Global Verification Log: User=%s, Password=%s", clientHello.User, clientHello.Password)

	// Send Server Hello
	serverHello := proto.ServerHello{
		Name:        "MockServer",
		Major:       22,
		Minor:       8,
		Revision:    54460,
		Timezone:    "UTC",
		DisplayName: "Mock",
		Patch:       1,
	}

	var buf proto.Buffer
	serverHello.EncodeAware(&buf, clientHello.ProtocolVersion)
	if _, err := conn.Write(buf.Buf); err != nil {
		log.Println("write server hello:", err)
		return
	}

	// Just consume and exit/respond to keep client happy
	for {
		packetType, err := r.UVarInt()
		if err != nil {
			return
		}

		switch proto.ClientCode(packetType) {
		case proto.ClientCodeQuery: // Query = 1
			buf.Reset()
			buf.PutByte(5) // EndOfStream
			if _, err := conn.Write(buf.Buf); err != nil {
				return
			}
		default:
			// ignore
		}
	}
}
