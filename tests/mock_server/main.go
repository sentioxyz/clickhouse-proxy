package main

import (
	"flag"
	"io"
	"log"
	"net"
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

// readVarInt reads a varint from the connection
func readVarInt(conn net.Conn) (uint64, error) {
	var value uint64
	var shift uint
	for {
		b := make([]byte, 1)
		if _, err := io.ReadFull(conn, b); err != nil {
			return 0, err
		}
		value |= uint64(b[0]&0x7f) << shift
		if b[0]&0x80 == 0 {
			break
		}
		shift += 7
	}
	return value, nil
}

// readString reads a length-prefixed string from the connection
func readString(conn net.Conn) (string, error) {
	length, err := readVarInt(conn)
	if err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return "", err
	}
	return string(data), nil
}

// writeVarInt writes a varint to the buffer
func writeVarInt(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	buf = append(buf, byte(v))
	return buf
}

// writeString writes a length-prefixed string
func writeString(buf []byte, s string) []byte {
	buf = writeVarInt(buf, uint64(len(s)))
	buf = append(buf, s...)
	return buf
}

func handle(conn net.Conn) {
	defer conn.Close()

	// Step 1: Read ClientHello
	// Format: [PacketType=0] [Name] [Major] [Minor] [ProtocolVersion] [Database] [User] [Password]

	// Packet Type (varint, should be 0 for Hello)
	packetType, err := readVarInt(conn)
	if err != nil {
		log.Println("read packet type:", err)
		return
	}
	if packetType != 0 {
		log.Println("expected Hello packet type 0, got", packetType)
		return
	}

	// Name (string)
	if _, err := readString(conn); err != nil {
		log.Println("read name:", err)
		return
	}

	// Major (varint)
	if _, err := readVarInt(conn); err != nil {
		log.Println("read major:", err)
		return
	}

	// Minor (varint)
	if _, err := readVarInt(conn); err != nil {
		log.Println("read minor:", err)
		return
	}

	// ProtocolVersion (varint)
	protoVersion, err := readVarInt(conn)
	if err != nil {
		log.Println("read proto version:", err)
		return
	}

	// Database (string)
	if _, err := readString(conn); err != nil {
		log.Println("read database:", err)
		return
	}

	// User (string)
	if _, err := readString(conn); err != nil {
		log.Println("read user:", err)
		return
	}

	// Password (string)
	if _, err := readString(conn); err != nil {
		log.Println("read password:", err)
		return
	}

	// Step 2: Send ServerHello
	var resp []byte
	resp = append(resp, 0)                 // Packet Type: Hello
	resp = writeString(resp, "MockServer") // Name
	resp = writeVarInt(resp, 22)           // Major
	resp = writeVarInt(resp, 8)            // Minor
	resp = writeVarInt(resp, protoVersion) // Revision (same as client)
	resp = writeString(resp, "UTC")        // Timezone
	resp = writeString(resp, "Mock")       // DisplayName

	// Patch version (if client supports it)
	if protoVersion >= 54448 {
		resp = writeVarInt(resp, 1) // Patch
	}

	if _, err := conn.Write(resp); err != nil {
		log.Println("write server hello:", err)
		return
	}

	// Step 3: Skip Addendum
	// For version 54460, only QuotaKey is present.

	// QuotaKey (FeatureQuotaKey >= 54458)
	if protoVersion >= 54458 {
		if _, err := readString(conn); err != nil {
			log.Println("skip quota key:", err)
			return
		}
	}

	// ChunkedPackets (FeatureChunkedPackets >= 54470)
	if protoVersion >= 54470 {
		if _, err := readString(conn); err != nil {
			log.Println("skip chunked send:", err)
			return
		}
		if _, err := readString(conn); err != nil {
			log.Println("skip chunked recv:", err)
			return
		}
	}

	// ParallelReplicasVersion (FeatureVersionedParallelReplicas >= 54471)
	if protoVersion >= 54471 {
		if _, err := readVarInt(conn); err != nil {
			log.Println("skip parallel replicas:", err)
			return
		}
	}

	// Step 4: Read Query packet and respond with EndOfStream
	// Query packet: [PacketType=1] [QueryID] [ClientInfo] [Settings] [Roles] [Secret] [Stage] [Compression] [Body] [Params]

	queryPacketType, err := readVarInt(conn)
	if err != nil {
		log.Println("read query packet type:", err)
		return
	}
	if queryPacketType != 1 {
		log.Println("expected Query packet type 1, got", queryPacketType)
		return
	}

	// Don't need to parse the rest of the Query, just send EndOfStream
	endOfStream := []byte{5} // ServerCodeEndOfStream = 5
	if _, err := conn.Write(endOfStream); err != nil {
		log.Println("write end of stream:", err)
		return
	}

	// Done with this request - connection will be closed
}
