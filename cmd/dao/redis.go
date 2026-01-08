package dao

import (
	"context"
	"fmt"
	"strings"

	"evm-scanner-go/cmd/config"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var RedisClient *redis.Client

// InitRedis 初始化Redis连接
func InitRedis(cfg *config.RedisConfig, log *zap.Logger) error {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     cfg.Addr(),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// 测试连接
	ctx := context.Background()
	if _, err := RedisClient.Ping(ctx).Result(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Info("Redis connected successfully",
		zap.String("addr", cfg.Addr()),
		zap.Int("db", cfg.DB))

	return nil
}

// CloseRedis 关闭Redis连接
func CloseRedis() error {
	if RedisClient != nil {
		return RedisClient.Close()
	}
	return nil
}

// AddressBloomFilter Bloom过滤器地址检查器
type AddressBloomFilter struct {
	client   *redis.Client
	bloomKey string
	log      *zap.Logger
}

// NewAddressBloomFilter 创建Bloom过滤器
func NewAddressBloomFilter(bloomKey string, log *zap.Logger) *AddressBloomFilter {
	return &AddressBloomFilter{
		client:   RedisClient,
		bloomKey: bloomKey,
		log:      log,
	}
}

// IsInternalAddress 检查地址是否为内部地址
// 使用Redis的BF.EXISTS命令（需要RedisBloom模块）
func (f *AddressBloomFilter) IsInternalAddress(ctx context.Context, address string) (bool, error) {
	// 统一转换为小写
	address = strings.ToLower(address)

	// 使用Redis Bloom Filter的BF.EXISTS命令
	result, err := f.client.Do(ctx, "BF.EXISTS", f.bloomKey, address).Int()
	if err != nil {
		// 如果Redis Bloom模块不可用，回退到普通SET
		return f.fallbackCheck(ctx, address)
	}

	return result == 1, nil
}

// fallbackCheck 回退检查（使用普通SET）
func (f *AddressBloomFilter) fallbackCheck(ctx context.Context, address string) (bool, error) {
	// 使用SISMEMBER检查地址是否在集合中
	result, err := f.client.SIsMember(ctx, f.bloomKey, address).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check address in set: %w", err)
	}
	return result, nil
}

// AddAddress 添加地址到Bloom过滤器
func (f *AddressBloomFilter) AddAddress(ctx context.Context, address string) error {
	address = strings.ToLower(address)

	// 尝试使用BF.ADD
	_, err := f.client.Do(ctx, "BF.ADD", f.bloomKey, address).Result()
	if err != nil {
		// 回退到普通SET
		return f.client.SAdd(ctx, f.bloomKey, address).Err()
	}
	return nil
}

// AddAddresses 批量添加地址到Bloom过滤器
func (f *AddressBloomFilter) AddAddresses(ctx context.Context, addresses []string) error {
	if len(addresses) == 0 {
		return nil
	}

	// 转换为小写
	lowerAddresses := make([]interface{}, len(addresses))
	for i, addr := range addresses {
		lowerAddresses[i] = strings.ToLower(addr)
	}

	// 尝试使用BF.MADD
	args := append([]interface{}{"BF.MADD", f.bloomKey}, lowerAddresses...)
	_, err := f.client.Do(ctx, args...).Result()
	if err != nil {
		// 回退到普通SET
		return f.client.SAdd(ctx, f.bloomKey, lowerAddresses...).Err()
	}
	return nil
}

// CreateBloomFilter 创建Bloom过滤器（如果不存在）
func (f *AddressBloomFilter) CreateBloomFilter(ctx context.Context, errorRate float64, capacity int64) error {
	// 检查是否已存在
	exists, err := f.client.Exists(ctx, f.bloomKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check bloom filter existence: %w", err)
	}

	if exists == 0 {
		// 创建Bloom过滤器 BF.RESERVE key error_rate capacity
		_, err := f.client.Do(ctx, "BF.RESERVE", f.bloomKey, errorRate, capacity).Result()
		if err != nil {
			f.log.Warn("Failed to create bloom filter, will use SET fallback",
				zap.Error(err))
			return nil
		}
		f.log.Info("Bloom filter created",
			zap.String("key", f.bloomKey),
			zap.Float64("error_rate", errorRate),
			zap.Int64("capacity", capacity))
	}

	return nil
}

// CheckAddresses 批量检查地址
func (f *AddressBloomFilter) CheckAddresses(ctx context.Context, addresses []string) (map[string]bool, error) {
	result := make(map[string]bool, len(addresses))

	for _, addr := range addresses {
		isInternal, err := f.IsInternalAddress(ctx, addr)
		if err != nil {
			return nil, err
		}
		result[strings.ToLower(addr)] = isInternal
	}

	return result, nil
}

// GetBloomInfo 获取Bloom过滤器信息
func (f *AddressBloomFilter) GetBloomInfo(ctx context.Context) (map[string]interface{}, error) {
	result, err := f.client.Do(ctx, "BF.INFO", f.bloomKey).Result()
	if err != nil {
		// 如果不支持Bloom，返回SET的大小
		size, err := f.client.SCard(ctx, f.bloomKey).Result()
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			"type": "set",
			"size": size,
		}, nil
	}

	return map[string]interface{}{
		"type": "bloom",
		"info": result,
	}, nil
}
