//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"time"

	pb "ck_remote_proxy/protos"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fmt.Println("Connecting to localhost:50051...")
	conn, err := grpc.DialContext(ctx, "localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()
	fmt.Println("✅ Connected!")

	client := pb.NewRewriterServiceClient(conn)
	req := &pb.RewriteSQLRequest{
		Sql:     "SELECT * FROM sentio_dvBsZtMc.GasSpent LIMIT 10",
		Options: nil,
	}

	fmt.Println("Sending Rewrite RPC (empty options)...")
	resp, err := client.Rewrite(ctx, req)
	if err != nil {
		fmt.Printf("❌ RPC failed: %v\n", err)
		return
	}
	fmt.Printf("✅ Response:\n  code=%v\n  sql_after=%s\n  tables=%v\n  message=%s\n",
		resp.Code, resp.SqlAfterRewrite, resp.OriginalAccessedTableNames, resp.Message)
}
