package services

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"evm-scanner-go/cmd/config"
	"evm-scanner-go/cmd/dao"
	"evm-scanner-go/cmd/internal"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"go.uber.org/zap"
)

// ERC20 Transfer事件签名
var transferEventSig = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

// blockTask 区块任务
type blockTask struct {
	blockNum uint64
}

// blockResult 区块处理结果
type blockResult struct {
	blockNum uint64
	err      error
}

// Scanner 并发区块扫描器
type Scanner struct {
	rpcPool     *RPCPool
	cfg         *config.ChainConfig
	bloomFilter *dao.AddressBloomFilter
	txDAO       *dao.TransactionDAO
	mqProducer  *MQProducer
	tracer      *Tracer // 内部转账追踪器
	log         *zap.Logger

	currentBlock uint64
	latestBlock  uint64

	// 并发控制
	blockWorkers  int
	txWorkers     int
	maxRetries    int
	retryInterval time.Duration

	// trace 配置
	traceEnabled bool

	// 统计信息
	processedBlocks   uint64
	processedTxs      uint64
	processedInternal uint64 // 内部转账数量
	startTime         time.Time

	stopCh chan struct{}
	wg     sync.WaitGroup
	mu     sync.RWMutex
}

// NewScanner 创建并发扫描器
func NewScanner(
	cfg *config.ChainConfig,
	bloomFilter *dao.AddressBloomFilter,
	txDAO *dao.TransactionDAO,
	mqProducer *MQProducer,
	log *zap.Logger,
) (*Scanner, error) {
	// 创建 RPC 连接池
	rpcPool, err := NewRPCPool(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC pool: %w", err)
	}

	// 设置默认并发参数
	blockWorkers := cfg.BlockWorkers
	if blockWorkers <= 0 {
		blockWorkers = runtime.NumCPU() * 2
	}

	txWorkers := cfg.TxWorkers
	if txWorkers <= 0 {
		txWorkers = runtime.NumCPU() * 4
	}

	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	retryIntervalMs := cfg.RetryIntervalMs
	if retryIntervalMs <= 0 {
		retryIntervalMs = 100
	}

	// 创建 tracer（如果启用）
	var tracer *Tracer
	traceEnabled := cfg.TraceEnabled
	if traceEnabled {
		tracer = NewTracer(cfg, log)
		// 检测节点支持的 trace 类型
		if cfg.TraceType == "" {
			supported, traceType, err := tracer.CheckTraceSupport(context.Background())
			if err != nil {
				log.Warn("Failed to check trace support", zap.Error(err))
				traceEnabled = false
			} else if supported {
				tracer.SetTraceType(traceType)
				log.Info("Trace API detected", zap.String("type", traceType))
			} else {
				log.Warn("Node does not support trace API, internal transfers will not be tracked")
				traceEnabled = false
			}
		}
	}

	log.Info("Scanner configured",
		zap.String("chain", cfg.Name),
		zap.Int64("chain_id", cfg.ChainID),
		zap.Int("block_workers", blockWorkers),
		zap.Int("tx_workers", txWorkers),
		zap.Int("rpc_pool_size", rpcPool.Size()),
		zap.Int("batch_size", cfg.BatchSize),
		zap.Bool("trace_enabled", traceEnabled))

	return &Scanner{
		rpcPool:       rpcPool,
		cfg:           cfg,
		bloomFilter:   bloomFilter,
		txDAO:         txDAO,
		mqProducer:    mqProducer,
		tracer:        tracer,
		log:           log,
		blockWorkers:  blockWorkers,
		txWorkers:     txWorkers,
		maxRetries:    maxRetries,
		traceEnabled:  traceEnabled,
		retryInterval: time.Duration(retryIntervalMs) * time.Millisecond,
		stopCh:        make(chan struct{}),
	}, nil
}

// Start 启动扫描
func (s *Scanner) Start(ctx context.Context) error {
	// 获取起始区块
	startBlock, err := s.getStartBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to get start block: %w", err)
	}
	s.currentBlock = startBlock
	s.startTime = time.Now()

	s.log.Info("Starting concurrent block scanner",
		zap.String("chain", s.cfg.Name),
		zap.Uint64("start_block", startBlock),
		zap.Int("block_workers", s.blockWorkers),
		zap.Int("tx_workers", s.txWorkers))

	s.wg.Add(1)
	go s.scanLoop(ctx)

	return nil
}

// Stop 停止扫描
func (s *Scanner) Stop() {
	close(s.stopCh)
	s.wg.Wait()
	s.rpcPool.Close()
	s.log.Info("Scanner stopped",
		zap.String("chain", s.cfg.Name),
		zap.Uint64("processed_blocks", atomic.LoadUint64(&s.processedBlocks)),
		zap.Uint64("processed_txs", atomic.LoadUint64(&s.processedTxs)))
}

// getStartBlock 获取起始区块
func (s *Scanner) getStartBlock(ctx context.Context) (uint64, error) {
	// 从数据库获取上次扫描的区块
	lastBlock, err := s.txDAO.GetLastScannedBlock(s.cfg.Name)
	if err != nil {
		return 0, err
	}

	if lastBlock > 0 {
		return lastBlock + 1, nil
	}

	// 如果配置了起始区块
	if s.cfg.StartBlock > 0 {
		return s.cfg.StartBlock, nil
	}

	// 否则从最新区块开始
	client := s.rpcPool.Get()
	latestBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return 0, err
	}

	return latestBlock, nil
}

// scanLoop 扫描循环
func (s *Scanner) scanLoop(ctx context.Context) {
	defer s.wg.Done()

	// 使用更短的轮询间隔
	pollInterval := time.Duration(s.cfg.PollInterval) * time.Second
	if pollInterval < 100*time.Millisecond {
		pollInterval = 100 * time.Millisecond
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// 立即开始第一次扫描
	if err := s.scanBlocksConcurrent(ctx); err != nil {
		s.log.Error("Failed to scan blocks", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			if err := s.scanBlocksConcurrent(ctx); err != nil {
				s.log.Error("Failed to scan blocks", zap.Error(err))
			}
		}
	}
}

// scanBlocksConcurrent 并发扫描区块
func (s *Scanner) scanBlocksConcurrent(ctx context.Context) error {
	// 获取最新区块
	client := s.rpcPool.Get()
	latestBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest block: %w", err)
	}

	s.mu.Lock()
	s.latestBlock = latestBlock
	s.mu.Unlock()

	// 计算安全区块（考虑确认数）
	safeBlock := latestBlock
	if latestBlock > s.cfg.Confirmations {
		safeBlock = latestBlock - s.cfg.Confirmations
	}

	// 批量扫描
	endBlock := s.currentBlock + uint64(s.cfg.BatchSize) - 1
	if endBlock > safeBlock {
		endBlock = safeBlock
	}

	if s.currentBlock > endBlock {
		return nil // 没有新区块
	}

	blocksToProcess := endBlock - s.currentBlock + 1

	s.log.Info("Scanning blocks concurrently",
		zap.Uint64("from", s.currentBlock),
		zap.Uint64("to", endBlock),
		zap.Uint64("count", blocksToProcess),
		zap.Uint64("latest", latestBlock),
		zap.Uint64("behind", latestBlock-endBlock))

	// 创建任务队列
	taskCh := make(chan blockTask, blocksToProcess)
	resultCh := make(chan blockResult, blocksToProcess)

	// 启动 worker
	var workerWg sync.WaitGroup
	workerCount := s.blockWorkers
	if int(blocksToProcess) < workerCount {
		workerCount = int(blocksToProcess)
	}

	for i := 0; i < workerCount; i++ {
		workerWg.Add(1)
		go s.blockWorker(ctx, i, taskCh, resultCh, &workerWg)
	}

	// 发送任务
	go func() {
		for blockNum := s.currentBlock; blockNum <= endBlock; blockNum++ {
			select {
			case <-ctx.Done():
				break
			case <-s.stopCh:
				break
			case taskCh <- blockTask{blockNum: blockNum}:
			}
		}
		close(taskCh)
	}()

	// 等待所有 worker 完成
	go func() {
		workerWg.Wait()
		close(resultCh)
	}()

	// 收集结果并按顺序更新进度
	results := make([]blockResult, 0, blocksToProcess)
	for result := range resultCh {
		results = append(results, result)
	}

	// 按区块号排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].blockNum < results[j].blockNum
	})

	// 按顺序更新进度（找到连续成功的最大区块）
	var lastSuccessBlock uint64 = s.currentBlock - 1
	var firstError error
	successCount := 0

	for _, result := range results {
		if result.err != nil {
			if firstError == nil {
				firstError = result.err
			}
			s.log.Error("Block processing failed",
				zap.Uint64("block", result.blockNum),
				zap.Error(result.err))
			// 停止在第一个错误处
			break
		}

		// 确保区块是连续的
		if result.blockNum == lastSuccessBlock+1 {
			lastSuccessBlock = result.blockNum
			successCount++

			// 更新进度
			if err := s.txDAO.UpdateScanProgress(s.cfg.Name, s.cfg.ChainID, result.blockNum); err != nil {
				s.log.Error("Failed to update scan progress", zap.Error(err))
			}
		} else {
			// 不连续，停止
			break
		}
	}

	// 更新当前区块
	if successCount > 0 {
		s.currentBlock = lastSuccessBlock + 1
		atomic.AddUint64(&s.processedBlocks, uint64(successCount))

		s.log.Info("Batch completed",
			zap.Int("success_count", successCount),
			zap.Uint64("last_block", lastSuccessBlock),
			zap.Uint64("next_block", s.currentBlock))
	}

	return firstError
}

// blockWorker 区块处理 worker
func (s *Scanner) blockWorker(ctx context.Context, id int, tasks <-chan blockTask, results chan<- blockResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for task := range tasks {
		select {
		case <-ctx.Done():
			results <- blockResult{blockNum: task.blockNum, err: ctx.Err()}
			continue
		case <-s.stopCh:
			results <- blockResult{blockNum: task.blockNum, err: fmt.Errorf("scanner stopped")}
			continue
		default:
		}

		// 带重试的区块处理
		var err error
		for retry := 0; retry <= s.maxRetries; retry++ {
			err = s.processBlockConcurrent(ctx, task.blockNum)
			if err == nil {
				break
			}

			if retry < s.maxRetries {
				s.log.Warn("Block processing failed, retrying",
					zap.Uint64("block", task.blockNum),
					zap.Int("retry", retry+1),
					zap.Error(err))
				time.Sleep(s.retryInterval)
			}
		}

		results <- blockResult{blockNum: task.blockNum, err: err}
	}
}

// processBlockConcurrent 并发处理单个区块
func (s *Scanner) processBlockConcurrent(ctx context.Context, blockNum uint64) error {
	client := s.rpcPool.Get()

	block, err := client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	blockTime := time.Unix(int64(block.Time()), 0)
	txs := block.Transactions()
	txCount := len(txs)

	s.log.Debug("Processing block",
		zap.Uint64("number", blockNum),
		zap.String("hash", block.Hash().Hex()),
		zap.Int("tx_count", txCount))

	if txCount == 0 {
		return nil
	}

	// 1. 批量获取所有交易的 receipt（预检查交易状态）
	receipts, successTxHashes := s.batchGetReceipts(ctx, txs)

	s.log.Debug("Receipts fetched",
		zap.Uint64("block", blockNum),
		zap.Int("total_txs", txCount),
		zap.Int("success_txs", len(successTxHashes)),
		zap.Int("failed_txs", txCount-len(successTxHashes)))

	// 2. 处理原生转账（包括成功和失败的交易，因为 gas 费还是会被扣除）
	if err := s.processTransactionsWithReceipts(ctx, txs, receipts, block, blockTime); err != nil {
		s.log.Error("Failed to process transactions",
			zap.Uint64("block", blockNum),
			zap.Error(err))
	}

	// 3. 处理 ERC20 Transfer 事件（只处理成功交易的日志）
	// FilterLogs 返回的日志已经是成功交易的，但我们传入 successTxHashes 用于双重验证
	if err := s.processTokenTransfersWithFilter(ctx, block, blockTime, successTxHashes); err != nil {
		s.log.Error("Failed to process token transfers",
			zap.Uint64("block", blockNum),
			zap.Error(err))
	}

	// 4. 处理合约内部转账（只处理成功的交易）
	if s.traceEnabled && len(successTxHashes) > 0 {
		if err := s.processInternalTransfersForSuccessTxs(ctx, block, blockTime, successTxHashes); err != nil {
			s.log.Error("Failed to process internal transfers",
				zap.Uint64("block", blockNum),
				zap.Error(err))
		}
	}

	return nil
}

// batchGetReceipts 批量获取交易回执
func (s *Scanner) batchGetReceipts(ctx context.Context, txs types.Transactions) (map[string]*types.Receipt, map[string]bool) {
	receipts := make(map[string]*types.Receipt)
	successTxHashes := make(map[string]bool)
	var mu sync.Mutex

	// 并发获取 receipt
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, s.txWorkers) // 控制并发数

	for _, tx := range txs {
		wg.Add(1)
		go func(tx *types.Transaction) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			txHash := tx.Hash()
			client := s.rpcPool.Get()

			// 带重试获取 receipt
			var receipt *types.Receipt
			var err error
			for retry := 0; retry <= s.maxRetries; retry++ {
				receipt, err = client.TransactionReceipt(ctx, txHash)
				if err == nil {
					break
				}
				if retry < s.maxRetries {
					time.Sleep(s.retryInterval)
					client = s.rpcPool.Get()
				}
			}

			if err != nil {
				s.log.Warn("Failed to get receipt",
					zap.String("tx_hash", txHash.Hex()),
					zap.Error(err))
				return
			}

			mu.Lock()
			receipts[txHash.Hex()] = receipt
			// 只有 status=1 的交易才是成功的
			if receipt.Status == 1 {
				successTxHashes[txHash.Hex()] = true
			}
			mu.Unlock()
		}(tx)
	}

	wg.Wait()
	return receipts, successTxHashes
}

// processTransactionsWithReceipts 使用预获取的 receipt 处理交易
func (s *Scanner) processTransactionsWithReceipts(ctx context.Context, txs types.Transactions, receipts map[string]*types.Receipt, block *types.Block, blockTime time.Time) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(txs))
	semaphore := make(chan struct{}, s.txWorkers)

	for _, tx := range txs {
		wg.Add(1)
		go func(tx *types.Transaction) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			receipt, ok := receipts[tx.Hash().Hex()]
			if !ok {
				// 如果没有预取到 receipt，重新获取
				client := s.rpcPool.Get()
				var err error
				receipt, err = client.TransactionReceipt(ctx, tx.Hash())
				if err != nil {
					errCh <- fmt.Errorf("failed to get receipt for %s: %w", tx.Hash().Hex(), err)
					return
				}
			}

			if err := s.processTransactionWithReceipt(ctx, tx, receipt, block, blockTime); err != nil {
				errCh <- err
			} else {
				atomic.AddUint64(&s.processedTxs, 1)
			}
		}(tx)
	}

	wg.Wait()
	close(errCh)

	// 收集错误
	var errCount int
	for err := range errCh {
		errCount++
		if errCount <= 5 {
			s.log.Warn("Transaction processing error", zap.Error(err))
		}
	}

	return nil
}

// processTransactionWithReceipt 使用预获取的 receipt 处理单个交易
func (s *Scanner) processTransactionWithReceipt(ctx context.Context, tx *types.Transaction, receipt *types.Receipt, block *types.Block, blockTime time.Time) error {
	// 获取发送者地址
	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		return fmt.Errorf("failed to get sender: %w", err)
	}

	// 获取接收者地址
	var to string
	if tx.To() != nil {
		to = strings.ToLower(tx.To().Hex())
	} else if receipt.ContractAddress != (common.Address{}) {
		to = strings.ToLower(receipt.ContractAddress.Hex())
	}

	fromAddr := strings.ToLower(from.Hex())

	// 检查地址是否为内部地址
	isFromInternal, _ := s.bloomFilter.IsInternalAddress(ctx, fromAddr)
	isToInternal := false
	if to != "" {
		isToInternal, _ = s.bloomFilter.IsInternalAddress(ctx, to)
	}

	// 确定交易类型
	txType := s.determineTxType(isFromInternal, isToInternal)

	// 构建交易对象
	transaction := &internal.Transaction{
		TxHash:          tx.Hash().Hex(),
		BlockNumber:     block.NumberU64(),
		BlockHash:       block.Hash().Hex(),
		BlockTime:       blockTime,
		From:            fromAddr,
		To:              to,
		Value:           tx.Value(),
		ValueStr:        tx.Value().String(),
		GasPrice:        tx.GasPrice(),
		GasUsed:         receipt.GasUsed,
		Nonce:           tx.Nonce(),
		TxIndex:         receipt.TransactionIndex,
		Input:           common.Bytes2Hex(tx.Data()),
		Status:          receipt.Status,
		ContractAddress: receipt.ContractAddress.Hex(),
		TxType:          txType,
		ChainID:         s.cfg.ChainID,
		ChainName:       s.cfg.Name,
		IsFromInternal:  isFromInternal,
		IsToInternal:    isToInternal,
	}

	// 发送到 RocketMQ
	if err := s.mqProducer.SendTransaction(ctx, transaction); err != nil {
		s.log.Error("Failed to send transaction to MQ",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.Error(err))
	}

	// 如果是相关交易，保存到数据库
	if txType != internal.TransactionTypeUnknown {
		if err := s.txDAO.SaveTransaction(transaction); err != nil {
			s.log.Error("Failed to save transaction",
				zap.String("tx_hash", tx.Hash().Hex()),
				zap.Error(err))
		} else {
			s.log.Info("Saved transaction",
				zap.String("tx_hash", tx.Hash().Hex()),
				zap.String("type", txType.String()),
				zap.Uint64("status", receipt.Status),
				zap.String("from", fromAddr),
				zap.String("to", to))
		}
	}

	return nil
}

// processTokenTransfersWithFilter 处理 ERC20 Transfer 事件（带成功交易过滤）
func (s *Scanner) processTokenTransfersWithFilter(ctx context.Context, block *types.Block, blockTime time.Time, successTxHashes map[string]bool) error {
	client := s.rpcPool.Get()

	query := ethereum.FilterQuery{
		FromBlock: block.Number(),
		ToBlock:   block.Number(),
		Topics:    [][]common.Hash{{transferEventSig}},
	}

	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to filter logs: %w", err)
	}

	if len(logs) == 0 {
		return nil
	}

	// 过滤只处理成功交易的日志
	var successLogs []types.Log
	for _, log := range logs {
		if successTxHashes[log.TxHash.Hex()] {
			successLogs = append(successLogs, log)
		}
	}

	s.log.Debug("Processing token transfers",
		zap.Uint64("block", block.NumberU64()),
		zap.Int("total_logs", len(logs)),
		zap.Int("success_logs", len(successLogs)))

	// 并发处理日志
	logCount := len(successLogs)
	if logCount == 0 {
		return nil
	}

	workerCount := s.txWorkers
	if logCount < workerCount {
		workerCount = logCount
	}

	type logTask struct {
		log         types.Log
		blockNumber uint64
		blockTime   time.Time
	}

	taskCh := make(chan logTask, logCount)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				if err := s.processTransferLog(ctx, task.log, task.blockNumber, task.blockTime); err != nil {
					s.log.Error("Failed to process transfer log",
						zap.String("tx_hash", task.log.TxHash.Hex()),
						zap.Uint("log_index", task.log.Index),
						zap.Error(err))
				}
			}
		}()
	}

	for _, log := range successLogs {
		taskCh <- logTask{
			log:         log,
			blockNumber: block.NumberU64(),
			blockTime:   blockTime,
		}
	}
	close(taskCh)

	wg.Wait()
	return nil
}

// processInternalTransfersForSuccessTxs 只处理成功交易的内部转账
func (s *Scanner) processInternalTransfersForSuccessTxs(ctx context.Context, block *types.Block, blockTime time.Time, successTxHashes map[string]bool) error {
	if s.tracer == nil {
		return nil
	}

	blockNumber := block.NumberU64()
	blockHash := block.Hash().Hex()

	// 尝试批量 trace 整个区块
	transfers, err := s.tracer.TraceBlock(ctx, blockNumber, blockHash, blockTime)
	if err != nil {
		// 批量 trace 失败，逐个交易 trace（只 trace 成功的交易）
		s.log.Warn("Batch trace failed, falling back to individual tx trace",
			zap.Uint64("block", blockNumber),
			zap.Error(err))

		for _, tx := range block.Transactions() {
			txHash := tx.Hash().Hex()
			// 只处理成功的交易
			if !successTxHashes[txHash] {
				continue
			}

			txTransfers, err := s.tracer.TraceTransaction(ctx, txHash, blockNumber, blockHash, blockTime)
			if err != nil {
				s.log.Debug("Failed to trace transaction",
					zap.String("tx_hash", txHash),
					zap.Error(err))
				continue
			}

			if len(txTransfers) > 0 {
				s.processInternalTransferList(ctx, txTransfers)
			}
		}
		return nil
	}

	// 处理批量 trace 结果（只处理成功交易的内部转账）
	for txHash, txTransfers := range transfers {
		if successTxHashes[txHash] {
			s.processInternalTransferList(ctx, txTransfers)
		}
	}

	return nil
}

// txTask 交易任务
// processTransferLog 处理单个 Transfer 日志
func (s *Scanner) processTransferLog(ctx context.Context, log types.Log, blockNumber uint64, blockTime time.Time) error {
	if len(log.Topics) != 3 {
		return nil
	}

	from := strings.ToLower(common.BytesToAddress(log.Topics[1].Bytes()).Hex())
	to := strings.ToLower(common.BytesToAddress(log.Topics[2].Bytes()).Hex())
	value := new(big.Int).SetBytes(log.Data)

	isFromInternal, _ := s.bloomFilter.IsInternalAddress(ctx, from)
	isToInternal, _ := s.bloomFilter.IsInternalAddress(ctx, to)

	txType := s.determineTxType(isFromInternal, isToInternal)

	transfer := &internal.TokenTransfer{
		TxHash:          log.TxHash.Hex(),
		LogIndex:        log.Index,
		BlockNumber:     blockNumber,
		ContractAddress: strings.ToLower(log.Address.Hex()),
		From:            from,
		To:              to,
		Value:           value,
		ValueStr:        value.String(),
		TxType:          txType,
		IsFromInternal:  isFromInternal,
		IsToInternal:    isToInternal,
	}

	// 发送到 RocketMQ
	if err := s.mqProducer.SendTokenTransfer(ctx, transfer); err != nil {
		s.log.Error("Failed to send token transfer to MQ",
			zap.String("tx_hash", log.TxHash.Hex()),
			zap.Error(err))
	}

	// 如果是相关交易，保存到数据库
	if txType != internal.TransactionTypeUnknown {
		if err := s.txDAO.SaveTokenTransfer(transfer, s.cfg.ChainID, s.cfg.Name); err != nil {
			s.log.Error("Failed to save token transfer",
				zap.String("tx_hash", log.TxHash.Hex()),
				zap.Error(err))
		} else {
			s.log.Info("Saved token transfer",
				zap.String("tx_hash", log.TxHash.Hex()),
				zap.String("contract", transfer.ContractAddress),
				zap.String("type", txType.String()))
		}
	}

	return nil
}

// determineTxType 确定交易类型
func (s *Scanner) determineTxType(isFromInternal, isToInternal bool) internal.TransactionType {
	if isFromInternal && !isToInternal {
		return internal.TransactionTypeWithdraw
	}
	if !isFromInternal && isToInternal {
		return internal.TransactionTypeDeposit
	}
	if isFromInternal && isToInternal {
		return internal.TransactionTypeInternal
	}
	return internal.TransactionTypeUnknown
}

// processInternalTransferList 处理内部转账列表
func (s *Scanner) processInternalTransferList(ctx context.Context, transfers []*internal.InternalTransfer) {
	for _, transfer := range transfers {
		// 跳过深度 0 的调用（这是主交易本身，不是内部转账）
		if transfer.Depth == 0 {
			continue
		}

		// 检查地址是否为内部地址
		isFromInternal, _ := s.bloomFilter.IsInternalAddress(ctx, transfer.From)
		isToInternal, _ := s.bloomFilter.IsInternalAddress(ctx, transfer.To)

		// 确定交易类型
		txType := s.determineTxType(isFromInternal, isToInternal)

		transfer.TxType = txType
		transfer.IsFromInternal = isFromInternal
		transfer.IsToInternal = isToInternal

		// 发送到 RocketMQ
		if err := s.mqProducer.SendInternalTransfer(ctx, transfer); err != nil {
			s.log.Error("Failed to send internal transfer to MQ",
				zap.String("tx_hash", transfer.TxHash),
				zap.Int("trace_index", transfer.TraceIndex),
				zap.Error(err))
		}

		// 如果是相关交易，保存到数据库
		if txType != internal.TransactionTypeUnknown {
			if err := s.txDAO.SaveInternalTransfer(transfer); err != nil {
				s.log.Error("Failed to save internal transfer",
					zap.String("tx_hash", transfer.TxHash),
					zap.Int("trace_index", transfer.TraceIndex),
					zap.Error(err))
			} else {
				s.log.Info("Saved internal transfer",
					zap.String("tx_hash", transfer.TxHash),
					zap.String("type", txType.String()),
					zap.String("from", transfer.From),
					zap.String("to", transfer.To),
					zap.String("value", transfer.ValueStr),
					zap.Int("depth", transfer.Depth))
			}
		}

		atomic.AddUint64(&s.processedInternal, 1)
	}
}

// GetStatus 获取扫描状态
func (s *Scanner) GetStatus() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	uptime := time.Since(s.startTime)
	blocksProcessed := atomic.LoadUint64(&s.processedBlocks)
	txsProcessed := atomic.LoadUint64(&s.processedTxs)
	internalProcessed := atomic.LoadUint64(&s.processedInternal)

	var blocksPerSecond, txsPerSecond float64
	if uptime.Seconds() > 0 {
		blocksPerSecond = float64(blocksProcessed) / uptime.Seconds()
		txsPerSecond = float64(txsProcessed) / uptime.Seconds()
	}

	// 获取 RPC 连接池统计
	rpcStats := s.rpcPool.GetStats()

	return map[string]interface{}{
		"chain_name":             s.cfg.Name,
		"chain_id":               s.cfg.ChainID,
		"current_block":          s.currentBlock,
		"latest_block":           s.latestBlock,
		"behind":                 s.latestBlock - s.currentBlock,
		"processed_blocks":       blocksProcessed,
		"processed_txs":          txsProcessed,
		"processed_internal_txs": internalProcessed,
		"blocks_per_second":      fmt.Sprintf("%.2f", blocksPerSecond),
		"txs_per_second":         fmt.Sprintf("%.2f", txsPerSecond),
		"uptime":                 uptime.String(),
		"block_workers":          s.blockWorkers,
		"tx_workers":             s.txWorkers,
		"trace_enabled":          s.traceEnabled,
		"rpc_pool":               rpcStats,
	}
}

// GetClient 获取一个 RPC 客户端（用于外部调用）
func (s *Scanner) GetClient() *ethclient.Client {
	return s.rpcPool.Get()
}
