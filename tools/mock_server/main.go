package main

import (
	"flag"
	"log"
	"net"

	"github.com/ClickHouse/ch-go/proto"
)

func main() {
	addr := flag.String("addr", ":9001", "listen address")
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
	// Simple handshake: Read ClientHello, Write ServerHello.
	// We use ch-go/proto to decode/encode.
	
	// Create a buffered reader to help decoding
    // Note: In a real server we need to handle buffering carefully, 
    // but here we just want to get past the handshake.
	r := proto.NewReader(conn)
	var clientHello proto.ClientHello
	if err := clientHello.Decode(r); err != nil {
		log.Println("decode client hello:", err)
		return
	}
	// log.Printf("Client Hello: %s (%v)", clientHello.Name, clientHello.ProtocolVersion)

	// Send Server Hello
	serverHello := proto.ServerHello{
		Name:            "MockServer",
		Major:           22,
		Minor:           8,
		Revision:        54460,
		Timezone:        "UTC",
		DisplayName:     "Mock",
		Patch:           1,
	}
	
	// Write server hello to buffer
	var buf proto.Buffer
	serverHello.EncodeAware(&buf, clientHello.ProtocolVersion)
	if _, err := conn.Write(buf.Buf); err != nil {
		log.Println("write server hello:", err)
		return
	}

	// Read packets and respond with EndOfStream
	for {
		packetType, err := r.UVarInt()
		if err != nil {
			// Connection closed or error
			return
		}

		// Read the rest of the packet data (just discard it)
		// For Query packets (type 1), we need to send EndOfStream back
		// For Data packets (type 2), we also respond
		switch proto.ClientCode(packetType) {
		case proto.ClientCodeQuery: // Query = 1
			// Skip reading query details, just drain whatever is available
			// and send back EndOfStream
			buf.Reset()
			// ServerCodeEndOfStream = 5
			buf.PutByte(5)
			if _, err := conn.Write(buf.Buf); err != nil {
				return
			}
		case proto.ClientCodeData: // Data = 2
			// For INSERT data, respond with EndOfStream
			buf.Reset()
			buf.PutByte(5)
			if _, err := conn.Write(buf.Buf); err != nil {
				return
			}
		case proto.ClientCodePing: // Ping = 4
			// Respond with Pong
			buf.Reset()
			buf.PutByte(7) // ServerCodePong
			if _, err := conn.Write(buf.Buf); err != nil {
				return
			}
		default:
			// Unknown packet, skip
		}
	}
}

