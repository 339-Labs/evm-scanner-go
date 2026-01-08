package services

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"evm-scanner-go/cmd/config"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
	"go.uber.org/zap"
)

// RPCService 链上 RPC 服务
type RPCService struct {
	client  *ethclient.Client
	cfg     *config.ChainConfig
	log     *zap.Logger
	chainID *big.Int
}

// NewRPCService 创建 RPC 服务
func NewRPCService(cfg *config.ChainConfig, log *zap.Logger) (*RPCService, error) {
	client, err := ethclient.Dial(cfg.RPCURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC: %w", err)
	}

	chainID, err := client.ChainID(context.Background())
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	if chainID.Int64() != cfg.ChainID {
		client.Close()
		return nil, fmt.Errorf("chain ID mismatch: expected %d, got %d", cfg.ChainID, chainID.Int64())
	}

	log.Info("RPC Service connected",
		zap.String("chain", cfg.Name),
		zap.Int64("chain_id", cfg.ChainID))

	return &RPCService{
		client:  client,
		cfg:     cfg,
		log:     log,
		chainID: chainID,
	}, nil
}

// Close 关闭连接
func (s *RPCService) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

// GetChainID 获取链 ID
func (s *RPCService) GetChainID() *big.Int {
	return s.chainID
}

// SendRawTransaction 发送原始交易
// rawTx 是签名后的交易数据（十六进制字符串，可带或不带 0x 前缀）
func (s *RPCService) SendRawTransaction(ctx context.Context, rawTx string) (string, error) {
	// 去除 0x 前缀
	rawTx = strings.TrimPrefix(rawTx, "0x")

	// 解码十六进制
	txBytes, err := hex.DecodeString(rawTx)
	if err != nil {
		return "", fmt.Errorf("invalid hex string: %w", err)
	}

	// 解码交易
	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(txBytes, tx); err != nil {
		return "", fmt.Errorf("failed to decode transaction: %w", err)
	}

	// 发送交易
	if err := s.client.SendTransaction(ctx, tx); err != nil {
		s.log.Error("Failed to send transaction",
			zap.String("tx_hash", tx.Hash().Hex()),
			zap.Error(err))
		return "", fmt.Errorf("failed to send transaction: %w", err)
	}

	s.log.Info("Transaction sent successfully",
		zap.String("tx_hash", tx.Hash().Hex()))

	return tx.Hash().Hex(), nil
}

// TransactionReceipt 交易回执信息
type TransactionReceipt struct {
	TxHash            string    `json:"tx_hash"`
	BlockNumber       uint64    `json:"block_number"`
	BlockHash         string    `json:"block_hash"`
	TransactionIndex  uint      `json:"transaction_index"`
	From              string    `json:"from"`
	To                string    `json:"to"`
	ContractAddress   string    `json:"contract_address,omitempty"`
	GasUsed           uint64    `json:"gas_used"`
	CumulativeGasUsed uint64    `json:"cumulative_gas_used"`
	EffectiveGasPrice string    `json:"effective_gas_price"`
	Status            uint64    `json:"status"` // 1=成功, 0=失败
	LogsCount         int       `json:"logs_count"`
	Logs              []LogInfo `json:"logs,omitempty"`
}

// LogInfo 日志信息
type LogInfo struct {
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	LogIndex    uint     `json:"log_index"`
	BlockNumber uint64   `json:"block_number"`
}

// GetTransactionReceipt 获取交易回执
func (s *RPCService) GetTransactionReceipt(ctx context.Context, txHash string) (*TransactionReceipt, error) {
	hash := common.HexToHash(txHash)

	receipt, err := s.client.TransactionReceipt(ctx, hash)
	if err != nil {
		if err == ethereum.NotFound {
			return nil, nil // 交易不存在或未被确认
		}
		return nil, fmt.Errorf("failed to get receipt: %w", err)
	}

	// 获取交易详情以获取 from 地址
	tx, _, err := s.client.TransactionByHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender: %w", err)
	}

	result := &TransactionReceipt{
		TxHash:            receipt.TxHash.Hex(),
		BlockNumber:       receipt.BlockNumber.Uint64(),
		BlockHash:         receipt.BlockHash.Hex(),
		TransactionIndex:  receipt.TransactionIndex,
		From:              from.Hex(),
		GasUsed:           receipt.GasUsed,
		CumulativeGasUsed: receipt.CumulativeGasUsed,
		Status:            receipt.Status,
		LogsCount:         len(receipt.Logs),
	}

	if tx.To() != nil {
		result.To = tx.To().Hex()
	}

	if receipt.ContractAddress != (common.Address{}) {
		result.ContractAddress = receipt.ContractAddress.Hex()
	}

	if receipt.EffectiveGasPrice != nil {
		result.EffectiveGasPrice = receipt.EffectiveGasPrice.String()
	}

	// 转换日志
	for _, log := range receipt.Logs {
		topics := make([]string, len(log.Topics))
		for i, t := range log.Topics {
			topics[i] = t.Hex()
		}
		result.Logs = append(result.Logs, LogInfo{
			Address:     log.Address.Hex(),
			Topics:      topics,
			Data:        common.Bytes2Hex(log.Data),
			LogIndex:    log.Index,
			BlockNumber: log.BlockNumber,
		})
	}

	return result, nil
}

// TransactionDetail 交易详情
type TransactionDetail struct {
	TxHash           string `json:"tx_hash"`
	BlockNumber      uint64 `json:"block_number,omitempty"`
	BlockHash        string `json:"block_hash,omitempty"`
	TransactionIndex uint64 `json:"transaction_index,omitempty"`
	From             string `json:"from"`
	To               string `json:"to,omitempty"`
	Value            string `json:"value"`
	Gas              uint64 `json:"gas"`
	GasPrice         string `json:"gas_price"`
	MaxFeePerGas     string `json:"max_fee_per_gas,omitempty"`
	MaxPriorityFee   string `json:"max_priority_fee_per_gas,omitempty"`
	Nonce            uint64 `json:"nonce"`
	Input            string `json:"input"`
	ChainID          int64  `json:"chain_id"`
	Type             uint8  `json:"type"`
	IsPending        bool   `json:"is_pending"`

	// 如果交易已确认，包含回执信息
	Status  *uint64 `json:"status,omitempty"`
	GasUsed *uint64 `json:"gas_used,omitempty"`
}

// GetTransactionByHash 获取交易详情
func (s *RPCService) GetTransactionByHash(ctx context.Context, txHash string) (*TransactionDetail, error) {
	hash := common.HexToHash(txHash)

	tx, isPending, err := s.client.TransactionByHash(ctx, hash)
	if err != nil {
		if err == ethereum.NotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender: %w", err)
	}

	result := &TransactionDetail{
		TxHash:    tx.Hash().Hex(),
		From:      from.Hex(),
		Value:     tx.Value().String(),
		Gas:       tx.Gas(),
		Nonce:     tx.Nonce(),
		Input:     common.Bytes2Hex(tx.Data()),
		Type:      tx.Type(),
		IsPending: isPending,
	}

	if tx.To() != nil {
		result.To = tx.To().Hex()
	}

	if tx.ChainId() != nil {
		result.ChainID = tx.ChainId().Int64()
	}

	if tx.GasPrice() != nil {
		result.GasPrice = tx.GasPrice().String()
	}

	if tx.GasFeeCap() != nil {
		result.MaxFeePerGas = tx.GasFeeCap().String()
	}

	if tx.GasTipCap() != nil {
		result.MaxPriorityFee = tx.GasTipCap().String()
	}

	// 如果交易已确认，获取回执信息
	if !isPending {
		receipt, err := s.client.TransactionReceipt(ctx, hash)
		if err == nil && receipt != nil {
			result.BlockNumber = receipt.BlockNumber.Uint64()
			result.BlockHash = receipt.BlockHash.Hex()
			result.TransactionIndex = uint64(receipt.TransactionIndex)
			result.Status = &receipt.Status
			result.GasUsed = &receipt.GasUsed
		}
	}

	return result, nil
}

// GetTransactionStatus 获取交易状态
// 返回: pending, success, failed, not_found
func (s *RPCService) GetTransactionStatus(ctx context.Context, txHash string) (string, error) {
	hash := common.HexToHash(txHash)

	tx, isPending, err := s.client.TransactionByHash(ctx, hash)
	if err != nil {
		if err == ethereum.NotFound {
			return "not_found", nil
		}
		return "", fmt.Errorf("failed to get transaction: %w", err)
	}

	if tx == nil {
		return "not_found", nil
	}

	if isPending {
		return "pending", nil
	}

	// 获取回执查看状态
	receipt, err := s.client.TransactionReceipt(ctx, hash)
	if err != nil {
		return "", fmt.Errorf("failed to get receipt: %w", err)
	}

	if receipt.Status == 1 {
		return "success", nil
	}
	return "failed", nil
}

// GetBlockNumber 获取最新区块号
func (s *RPCService) GetBlockNumber(ctx context.Context) (uint64, error) {
	return s.client.BlockNumber(ctx)
}

// GetBalance 获取账户余额
func (s *RPCService) GetBalance(ctx context.Context, address string) (*big.Int, error) {
	addr := common.HexToAddress(address)
	return s.client.BalanceAt(ctx, addr, nil)
}

// GetNonce 获取账户 nonce
func (s *RPCService) GetNonce(ctx context.Context, address string) (uint64, error) {
	addr := common.HexToAddress(address)
	return s.client.PendingNonceAt(ctx, addr)
}

// GetGasPrice 获取当前 gas 价格
func (s *RPCService) GetGasPrice(ctx context.Context) (*big.Int, error) {
	return s.client.SuggestGasPrice(ctx)
}

// EstimateGas 估算交易 gas
func (s *RPCService) EstimateGas(ctx context.Context, from, to string, value *big.Int, data []byte) (uint64, error) {
	fromAddr := common.HexToAddress(from)
	toAddr := common.HexToAddress(to)

	msg := ethereum.CallMsg{
		From:  fromAddr,
		To:    &toAddr,
		Value: value,
		Data:  data,
	}

	return s.client.EstimateGas(ctx, msg)
}

// GetTransactionCount 获取地址的交易数量（已确认的 nonce）
func (s *RPCService) GetTransactionCount(ctx context.Context, address string) (uint64, error) {
	addr := common.HexToAddress(address)
	return s.client.NonceAt(ctx, addr, nil)
}

// CallContract 调用合约（只读）
func (s *RPCService) CallContract(ctx context.Context, to string, data []byte) ([]byte, error) {
	toAddr := common.HexToAddress(to)

	msg := ethereum.CallMsg{
		To:   &toAddr,
		Data: data,
	}

	return s.client.CallContract(ctx, msg, nil)
}
