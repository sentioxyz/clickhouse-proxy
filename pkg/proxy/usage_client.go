package proxy

import (
	"context"
	"fmt"
	"time"

	log "sentioxyz/sentio-core/common/log"
	usageProtos "sentioxyz/sentio-core/service/usage/protos"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	queryCheckCachePrefix = "query_check:" // key: query_check:{payer}:{signer}
	queryCheckCacheTTL    = 5 * time.Minute

	reportMaxRetries    = 5
	reportRetryBaseWait = 1 * time.Second
	reportRetryTimeout  = 10 * time.Second
)

// UsageClient handles query usage reporting and balance checking via sentio-node gRPC.
type UsageClient struct {
	grpcClient  usageProtos.QueryUsageServiceClient
	grpcConn    *grpc.ClientConn
	redisClient *redis.Client
}

// NewUsageClient creates a new UsageClient connected to the sentio-node QueryUsageService.
func NewUsageClient(sentioNodeAddr string, redisClient *redis.Client) (*UsageClient, error) {
	conn, err := grpc.NewClient(sentioNodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return &UsageClient{
		grpcClient:  usageProtos.NewQueryUsageServiceClient(conn),
		grpcConn:    conn,
		redisClient: redisClient,
	}, nil
}

// CheckBalance checks if a query should be allowed (balance + authorization).
// Results are cached in Redis with key query_check:{payer}:{signer}, TTL 5 min.
func (c *UsageClient) CheckBalance(ctx context.Context, payer string, signer string) (bool, usageProtos.CheckQueryBalanceRejection, error) {
	cacheKey := queryCheckCachePrefix + payer + ":" + signer

	// Check Redis cache
	if c.redisClient != nil {
		cached, err := c.redisClient.Get(ctx, cacheKey).Result()
		if err == nil {
			if cached == "ok" {
				return true, 0, nil
			}
			// cached value is the rejection reason number
			var reason usageProtos.CheckQueryBalanceRejection
			switch cached {
			case "1":
				reason = usageProtos.CheckQueryBalanceRejection_INSUFFICIENT_BALANCE
			case "2":
				reason = usageProtos.CheckQueryBalanceRejection_UNAUTHORIZED_SIGNER
			default:
				reason = usageProtos.CheckQueryBalanceRejection_UNKNOWN
			}
			return false, reason, nil
		}
	}

	// Call sentio-node
	resp, err := c.grpcClient.CheckQueryBalance(ctx, &usageProtos.CheckQueryBalanceRequest{
		Account: payer,
		Signer:  signer,
	})
	if err != nil {
		log.Warnf("[usage_client] CheckQueryBalance failed for payer=%s signer=%s: %v", payer, signer, err)
		// Fail open — don't cache
		return true, 0, nil
	}

	// Cache result in Redis
	if c.redisClient != nil {
		val := "ok"
		if !resp.Status {
			val = fmt.Sprintf("%d", int32(resp.Reason))
		}
		_ = c.redisClient.Set(ctx, cacheKey, val, queryCheckCacheTTL).Err()
	}

	return resp.Status, resp.Reason, nil
}

// ReportUsage reports query usage to sentio-node asynchronously with retries to guarantee delivery.
func (c *UsageClient) ReportUsage(ctx context.Context, payer string, signer string, amount uint64) {
	go func() {
		req := &usageProtos.ReportQueryUsageRequest{
			Account: payer,
			Amount:  amount,
			Signer:  signer,
		}
		wait := reportRetryBaseWait
		for attempt := 1; attempt <= reportMaxRetries; attempt++ {
			rctx, cancel := context.WithTimeout(context.Background(), reportRetryTimeout)
			_, err := c.grpcClient.ReportQueryUsage(rctx, req)
			cancel()
			if err == nil {
				return
			}
			if attempt == reportMaxRetries {
				log.Errorf("[usage_client] ReportQueryUsage failed after %d attempts for payer=%s signer=%s: %v", reportMaxRetries, payer, signer, err)
				return
			}
			log.Warnf("[usage_client] ReportQueryUsage attempt %d/%d failed for payer=%s signer=%s: %v, retrying in %v", attempt, reportMaxRetries, payer, signer, err, wait)
			time.Sleep(wait)
			wait *= 2
		}
	}()
}

// Close closes the gRPC connection.
func (c *UsageClient) Close() error {
	if c.grpcConn != nil {
		return c.grpcConn.Close()
	}
	return nil
}
