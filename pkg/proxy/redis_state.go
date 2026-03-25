package proxy

import (
	"context"
	"encoding/json"
	"fmt"

	log "sentioxyz/sentio-core/common/log"
	"sentioxyz/sentio-core/common/statemirror"
	"sentioxyz/sentio-core/network/registry"
	"sentioxyz/sentio-core/network/state"

	"github.com/redis/go-redis/v9"
)

// RedisNetworkState implements NetworkState by delegating to sentio-core's
// ProcessorRegistry, which reads from Redis statemirror (statemirror:v1:*).
// This ensures the proxy uses exactly the same data format and access pattern
// as the sentio analytic-service.
type RedisNetworkState struct {
	registry    registry.ProcessorRegistry
	redisClient *redis.Client
	mirror      statemirror.Mirror
}

// NewRedisNetworkState creates a NetworkState using an existing Redis client.
func NewRedisNetworkState(client *redis.Client) (*RedisNetworkState, error) {
	mirror := statemirror.NewRedisMirror(client)
	reg := registry.NewProcessorRegistry(mirror)

	return &RedisNetworkState{
		registry:    reg,
		redisClient: client,
		mirror:      mirror,
	}, nil
}

func (r *RedisNetworkState) GetProcessorAllocation(processorId string) ([]ProcessorAllocation, bool) {
	ctx := context.Background()
	allocs, err := r.registry.RetrieveProcessorAllocation(ctx, processorId)
	if err != nil {
		log.Warnf("Redis: failed to get processor allocation for %s: %v", processorId, err)
		return nil, false
	}
	// Convert sentio-core types to proxy types
	result := make([]ProcessorAllocation, len(allocs))
	for i, a := range allocs {
		result[i] = ProcessorAllocation{
			ProcessorId: a.ProcessorId,
			IndexerId:   a.IndexerId,
		}
	}
	return result, true
}

func (r *RedisNetworkState) GetIndexerInfo(indexerId uint64) (IndexerInfo, bool) {
	// ProcessorRegistry doesn't expose GetIndexerInfo directly.
	// We read from Redis mirror directly using the same key format.
	ctx := context.Background()
	field := fmt.Sprintf("%d", indexerId)
	value, ok, err := r.mirror.Get(ctx, statemirror.MappingIndexerInfos, field)
	if err != nil || !ok {
		return IndexerInfo{}, false
	}

	// Parse JSON
	var info state.IndexerInfo
	if err := decodeJSON(value, &info); err != nil {
		log.Warnf("Redis: failed to decode IndexerInfo for %d: %v", indexerId, err)
		return IndexerInfo{}, false
	}

	return IndexerInfo{
		IndexerId:           info.IndexerId,
		IndexerUrl:          info.IndexerUrl,
		ComputeNodeRpcPort:  info.ComputeNodeRpcPort,
		StorageNodeRpcPort:  info.StorageNodeRpcPort,
		ClickhouseProxyPort: info.ClickhouseProxyPort,
	}, true
}

func (r *RedisNetworkState) GetProcessorInfo(processorId string) (ProcessorInfo, bool) {
	ctx := context.Background()
	info, err := r.registry.RetrieveProcessorInfo(ctx, processorId)
	if err != nil {
		log.Warnf("Redis: failed to get processor info for %s: %v", processorId, err)
		return ProcessorInfo{}, false
	}
	return ProcessorInfo{
		ProcessorId:         info.ProcessorId,
		EntitySchema:        info.EntitySchema,
		EntitySchemaVersion: info.EntitySchemaVersion,
	}, true
}

func (r *RedisNetworkState) GetAllIndexerInfos() map[uint64]IndexerInfo {
	ctx := context.Background()
	all, err := r.mirror.GetAll(ctx, statemirror.MappingIndexerInfos)
	if err != nil {
		log.Warnf("Redis: failed to get all indexer infos: %v", err)
		return nil
	}
	result := make(map[uint64]IndexerInfo, len(all))
	for field, value := range all {
		id, err := ParseIndexerId(field)
		if err != nil {
			log.Warnf("Redis: failed to parse indexer id %q: %v", field, err)
			continue
		}
		var info state.IndexerInfo
		if err := decodeJSON(value, &info); err != nil {
			log.Warnf("Redis: failed to decode IndexerInfo for %s: %v", field, err)
			continue
		}
		result[id] = IndexerInfo{
			IndexerId:           info.IndexerId,
			IndexerUrl:          info.IndexerUrl,
			ComputeNodeRpcPort:  info.ComputeNodeRpcPort,
			StorageNodeRpcPort:  info.StorageNodeRpcPort,
			ClickhouseProxyPort: info.ClickhouseProxyPort,
		}
	}
	return result
}

// Close closes the Redis connection.
func (r *RedisNetworkState) Close() error {
	return r.redisClient.Close()
}

// decodeJSON unmarshals a JSON string into the target.
func decodeJSON(s string, target interface{}) error {
	return json.Unmarshal([]byte(s), target)
}
