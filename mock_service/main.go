package main

import (
	"context"
	"log"
	"net"
	"strings"

	pb "ck_remote_proxy/protos"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedRewriterServiceServer
}

func (s *server) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	sql := req.Sql
	log.Printf("Received Rewrite request: %s", sql)

	rewritten := sql
	// Simple mock logic matching our test cases
	if strings.Contains(sql, "sentio_coinbase.transfer") {
		rewritten = strings.ReplaceAll(rewritten, "sentio_coinbase.transfer", "sentio.coinbase_transfer")
	}
	if strings.Contains(sql, "sentio_pancakeswap123.Withdrawl") {
		// Mock remote rewrite
		rewritten = strings.ReplaceAll(rewritten, "sentio_pancakeswap123.Withdrawl", "remote('127.0.0.1:9000', 'sentio', 'pancakes_Withdrawl', 'default', 'password')")
	}

	log.Printf("Returning: %s", rewritten)

	return &pb.RewriteSQLResponse{
		SqlAfterRewrite: rewritten,
		Message:         "success",
		Code:            pb.RewriteCode_Success,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterRewriterServiceServer(s, &server{})
	log.Printf("Mock Rewriter Server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
