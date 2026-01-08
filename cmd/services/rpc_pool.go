package services

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"evm-scanner-go/cmd/config"

	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
)

// clientWrapper 连接包装器，包含连接状态
type clientWrapper struct {
	client    *ethclient.Client
	healthy   bool
	lastCheck time.Time
	failCount int
	mu        sync.RWMutex
}

// RPCPool RPC 连接池（带健康检查和自动重连）
type RPCPool struct {
	wrappers []*clientWrapper
	size     int
	index    uint64
	rpcURL   string
	chainID  int64
	log      *zap.Logger

	// 健康检查配置
	healthCheckInterval time.Duration
	maxFailCount        int
	reconnectInterval   time.Duration

	// 统计信息
	totalRequests  uint64
	failedRequests uint64
	reconnectCount uint64

	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// NewRPCPool 创建 RPC 连接池
func NewRPCPool(cfg *config.ChainConfig, log *zap.Logger) (*RPCPool, error) {
	size := cfg.RPCPoolSize
	if size <= 0 {
		size = 5 // 默认 5 个连接
	}

	pool := &RPCPool{
		wrappers:            make([]*clientWrapper, size),
		size:                size,
		rpcURL:              cfg.RPCURL,
		chainID:             cfg.ChainID,
		log:                 log,
		healthCheckInterval: 30 * time.Second, // 每 30 秒检查一次
		maxFailCount:        3,                // 连续失败 3 次标记为不健康
		reconnectInterval:   5 * time.Second,  // 重连间隔 5 秒
		stopCh:              make(chan struct{}),
	}

	// 创建连接
	for i := 0; i < size; i++ {
		client, err := ethclient.Dial(cfg.RPCURL)
		if err != nil {
			// 关闭已创建的连接
			pool.Close()
			return nil, fmt.Errorf("failed to create RPC client %d: %w", i, err)
		}
		pool.wrappers[i] = &clientWrapper{
			client:    client,
			healthy:   true,
			lastCheck: time.Now(),
		}
	}

	// 验证第一个连接的 chainID
	chainID, err := pool.wrappers[0].client.ChainID(context.Background())
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	if chainID.Int64() != cfg.ChainID {
		pool.Close()
		return nil, fmt.Errorf("chain ID mismatch: expected %d, got %d", cfg.ChainID, chainID.Int64())
	}

	log.Info("RPC connection pool created",
		zap.Int("pool_size", size),
		zap.String("rpc_url", cfg.RPCURL),
		zap.Duration("health_check_interval", pool.healthCheckInterval))

	// 启动健康检查协程
	pool.wg.Add(1)
	go pool.healthCheckLoop()

	return pool, nil
}

// Get 获取一个健康的 RPC 客户端（轮询方式）
func (p *RPCPool) Get() *ethclient.Client {
	atomic.AddUint64(&p.totalRequests, 1)

	// 尝试获取健康的连接
	startIdx := atomic.AddUint64(&p.index, 1)
	for i := 0; i < p.size; i++ {
		idx := int(startIdx+uint64(i)) % p.size
		wrapper := p.wrappers[idx]

		wrapper.mu.RLock()
		healthy := wrapper.healthy
		client := wrapper.client
		wrapper.mu.RUnlock()

		if healthy && client != nil {
			return client
		}
	}

	// 没有健康的连接，返回任意一个（可能触发重连）
	p.log.Warn("No healthy RPC connection available, using fallback")
	atomic.AddUint64(&p.failedRequests, 1)

	idx := int(startIdx) % p.size
	return p.wrappers[idx].client
}

// GetWithContext 获取客户端并执行操作，自动处理错误和重试
func (p *RPCPool) GetWithContext(ctx context.Context, fn func(*ethclient.Client) error) error {
	var lastErr error

	// 尝试使用不同的连接
	for retry := 0; retry < p.size; retry++ {
		client := p.Get()
		if client == nil {
			continue
		}

		err := fn(client)
		if err == nil {
			return nil
		}

		lastErr = err

		// 标记连接可能不健康
		p.markClientError(client)

		// 检查是否是 context 取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	return lastErr
}

// markClientError 标记客户端发生错误
func (p *RPCPool) markClientError(client *ethclient.Client) {
	for _, wrapper := range p.wrappers {
		wrapper.mu.Lock()
		if wrapper.client == client {
			wrapper.failCount++
			if wrapper.failCount >= p.maxFailCount {
				wrapper.healthy = false
				p.log.Warn("RPC client marked as unhealthy",
					zap.Int("fail_count", wrapper.failCount))
			}
		}
		wrapper.mu.Unlock()
	}
}

// markClientSuccess 标记客户端请求成功
func (p *RPCPool) markClientSuccess(client *ethclient.Client) {
	for _, wrapper := range p.wrappers {
		wrapper.mu.Lock()
		if wrapper.client == client {
			wrapper.failCount = 0
			wrapper.healthy = true
		}
		wrapper.mu.Unlock()
	}
}

// healthCheckLoop 健康检查循环
func (p *RPCPool) healthCheckLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.checkAndReconnect()
		}
	}
}

// checkAndReconnect 检查连接健康状态并重连
func (p *RPCPool) checkAndReconnect() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i, wrapper := range p.wrappers {
		wrapper.mu.Lock()

		// 检查连接是否健康
		if wrapper.client != nil {
			_, err := wrapper.client.BlockNumber(ctx)
			if err != nil {
				wrapper.failCount++
				if wrapper.failCount >= p.maxFailCount {
					wrapper.healthy = false
					p.log.Warn("Health check failed, marking client as unhealthy",
						zap.Int("client_index", i),
						zap.Int("fail_count", wrapper.failCount),
						zap.Error(err))
				}
			} else {
				// 健康检查通过
				wrapper.failCount = 0
				wrapper.healthy = true
			}
		}

		// 尝试重连不健康的连接
		if !wrapper.healthy || wrapper.client == nil {
			p.log.Info("Attempting to reconnect RPC client", zap.Int("client_index", i))

			// 关闭旧连接
			if wrapper.client != nil {
				wrapper.client.Close()
			}

			// 创建新连接
			newClient, err := ethclient.Dial(p.rpcURL)
			if err != nil {
				p.log.Error("Failed to reconnect RPC client",
					zap.Int("client_index", i),
					zap.Error(err))
				wrapper.client = nil
				wrapper.healthy = false
			} else {
				// 验证新连接
				chainID, err := newClient.ChainID(ctx)
				if err != nil || chainID.Int64() != p.chainID {
					p.log.Error("Reconnected client failed chain ID verification",
						zap.Int("client_index", i),
						zap.Error(err))
					newClient.Close()
					wrapper.client = nil
					wrapper.healthy = false
				} else {
					wrapper.client = newClient
					wrapper.healthy = true
					wrapper.failCount = 0
					atomic.AddUint64(&p.reconnectCount, 1)
					p.log.Info("RPC client reconnected successfully",
						zap.Int("client_index", i))
				}
			}
		}

		wrapper.lastCheck = time.Now()
		wrapper.mu.Unlock()
	}
}

// GetHealthyCount 获取健康连接数量
func (p *RPCPool) GetHealthyCount() int {
	count := 0
	for _, wrapper := range p.wrappers {
		wrapper.mu.RLock()
		if wrapper.healthy && wrapper.client != nil {
			count++
		}
		wrapper.mu.RUnlock()
	}
	return count
}

// GetStats 获取连接池统计信息
func (p *RPCPool) GetStats() map[string]interface{} {
	healthyCount := p.GetHealthyCount()

	return map[string]interface{}{
		"pool_size":       p.size,
		"healthy_count":   healthyCount,
		"unhealthy_count": p.size - healthyCount,
		"total_requests":  atomic.LoadUint64(&p.totalRequests),
		"failed_requests": atomic.LoadUint64(&p.failedRequests),
		"reconnect_count": atomic.LoadUint64(&p.reconnectCount),
	}
}

// Size 获取连接池大小
func (p *RPCPool) Size() int {
	return p.size
}

// Close 关闭所有连接
func (p *RPCPool) Close() {
	// 停止健康检查
	close(p.stopCh)
	p.wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, wrapper := range p.wrappers {
		if wrapper != nil && wrapper.client != nil {
			wrapper.client.Close()
		}
	}
	p.wrappers = nil
	p.log.Info("RPC connection pool closed",
		zap.Uint64("total_requests", atomic.LoadUint64(&p.totalRequests)),
		zap.Uint64("reconnect_count", atomic.LoadUint64(&p.reconnectCount)))
}

// ForceReconnectAll 强制重连所有连接
func (p *RPCPool) ForceReconnectAll() {
	p.log.Info("Force reconnecting all RPC clients")
	for _, wrapper := range p.wrappers {
		wrapper.mu.Lock()
		wrapper.healthy = false
		wrapper.failCount = p.maxFailCount
		wrapper.mu.Unlock()
	}
	// 触发重连
	p.checkAndReconnect()
}

// BlockResult 区块处理结果
type BlockResult struct {
	BlockNumber uint64
	Block       interface{} // *types.Block
	Error       error
}

// TxResult 交易处理结果
type TxResult struct {
	TxHash string
	Error  error
}
