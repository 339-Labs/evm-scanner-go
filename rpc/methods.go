package rpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"evm-scanner-go/cmd/config"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
	"go.uber.org/zap"
)

// EthService 以太坊 RPC 方法服务
type EthService struct {
	client  *ethclient.Client
	cfg     *config.ChainConfig
	log     *zap.Logger
	chainID *big.Int
}

// NewEthService 创建以太坊服务
func NewEthService(cfg *config.ChainConfig, log *zap.Logger) (*EthService, error) {
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

	log.Info("Eth RPC Service connected",
		zap.String("chain", cfg.Name),
		zap.Int64("chain_id", cfg.ChainID))

	return &EthService{
		client:  client,
		cfg:     cfg,
		log:     log,
		chainID: chainID,
	}, nil
}

// Close 关闭连接
func (s *EthService) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

// RegisterMethods 注册所有 eth_* 方法
func (s *EthService) RegisterMethods(registry *MethodRegistry) {
	// 链信息
	registry.Register("eth_chainId", s.ChainId)
	registry.Register("eth_blockNumber", s.BlockNumber)
	registry.Register("eth_gasPrice", s.GasPrice)
	registry.Register("eth_maxPriorityFeePerGas", s.MaxPriorityFeePerGas)
	registry.Register("eth_feeHistory", s.FeeHistory)

	// 账户信息
	registry.Register("eth_getBalance", s.GetBalance)
	registry.Register("eth_getTransactionCount", s.GetTransactionCount)
	registry.Register("eth_getCode", s.GetCode)
	registry.Register("eth_getStorageAt", s.GetStorageAt)

	// 交易相关
	registry.Register("eth_sendRawTransaction", s.SendRawTransaction)
	registry.Register("eth_getTransactionByHash", s.GetTransactionByHash)
	registry.Register("eth_getTransactionReceipt", s.GetTransactionReceipt)
	registry.Register("eth_call", s.Call)
	registry.Register("eth_estimateGas", s.EstimateGas)

	// 区块信息
	registry.Register("eth_getBlockByNumber", s.GetBlockByNumber)
	registry.Register("eth_getBlockByHash", s.GetBlockByHash)
	registry.Register("eth_getBlockTransactionCountByNumber", s.GetBlockTransactionCountByNumber)
	registry.Register("eth_getBlockTransactionCountByHash", s.GetBlockTransactionCountByHash)

	// 日志
	registry.Register("eth_getLogs", s.GetLogs)

	// 网络信息
	registry.Register("net_version", s.NetVersion)
	registry.Register("net_listening", s.NetListening)

	// Web3
	registry.Register("web3_clientVersion", s.Web3ClientVersion)

	s.log.Info("Registered eth RPC methods", zap.Int("count", len(registry.Methods())))
}

// ========== 链信息方法 ==========

// ChainId eth_chainId
func (s *EthService) ChainId(params json.RawMessage) (interface{}, *Error) {
	return hexutil.EncodeBig(s.chainID), nil
}

// BlockNumber eth_blockNumber
func (s *EthService) BlockNumber(params json.RawMessage) (interface{}, *Error) {
	blockNum, err := s.client.BlockNumber(context.Background())
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}
	return hexutil.EncodeUint64(blockNum), nil
}

// GasPrice eth_gasPrice
func (s *EthService) GasPrice(params json.RawMessage) (interface{}, *Error) {
	gasPrice, err := s.client.SuggestGasPrice(context.Background())
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}
	return hexutil.EncodeBig(gasPrice), nil
}

// MaxPriorityFeePerGas eth_maxPriorityFeePerGas
func (s *EthService) MaxPriorityFeePerGas(params json.RawMessage) (interface{}, *Error) {
	tip, err := s.client.SuggestGasTipCap(context.Background())
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}
	return hexutil.EncodeBig(tip), nil
}

// FeeHistory eth_feeHistory
func (s *EthService) FeeHistory(params json.RawMessage) (interface{}, *Error) {
	var args []interface{}
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 3 {
		return nil, ErrInvalidParams
	}

	blockCount, err := hexutil.DecodeUint64(args[0].(string))
	if err != nil {
		return nil, ErrInvalidParams
	}

	newestBlock := args[1].(string)
	var blockNum *big.Int
	if newestBlock == "latest" || newestBlock == "pending" {
		blockNum = nil
	} else {
		num, err := hexutil.DecodeBig(newestBlock)
		if err != nil {
			return nil, ErrInvalidParams
		}
		blockNum = num
	}

	var rewardPercentiles []float64
	if percents, ok := args[2].([]interface{}); ok {
		for _, p := range percents {
			if f, ok := p.(float64); ok {
				rewardPercentiles = append(rewardPercentiles, f)
			}
		}
	}

	history, err := s.client.FeeHistory(context.Background(), blockCount, blockNum, rewardPercentiles)
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	return history, nil
}

// ========== 账户信息方法 ==========

// GetBalance eth_getBalance
func (s *EthService) GetBalance(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	address := common.HexToAddress(args[0])
	blockNum := s.parseBlockNumber(args, 1)

	balance, err := s.client.BalanceAt(context.Background(), address, blockNum)
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	return hexutil.EncodeBig(balance), nil
}

// GetTransactionCount eth_getTransactionCount
func (s *EthService) GetTransactionCount(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	address := common.HexToAddress(args[0])

	var nonce uint64
	var err error

	if len(args) > 1 && args[1] == "pending" {
		nonce, err = s.client.PendingNonceAt(context.Background(), address)
	} else {
		blockNum := s.parseBlockNumber(args, 1)
		nonce, err = s.client.NonceAt(context.Background(), address, blockNum)
	}

	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	return hexutil.EncodeUint64(nonce), nil
}

// GetCode eth_getCode
func (s *EthService) GetCode(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	address := common.HexToAddress(args[0])
	blockNum := s.parseBlockNumber(args, 1)

	code, err := s.client.CodeAt(context.Background(), address, blockNum)
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	return hexutil.Encode(code), nil
}

// GetStorageAt eth_getStorageAt
func (s *EthService) GetStorageAt(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 2 {
		return nil, ErrInvalidParams
	}

	address := common.HexToAddress(args[0])
	slot := common.HexToHash(args[1])
	blockNum := s.parseBlockNumber(args, 2)

	storage, err := s.client.StorageAt(context.Background(), address, slot, blockNum)
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	return hexutil.Encode(storage), nil
}

// ========== 交易方法 ==========

// SendRawTransaction eth_sendRawTransaction
func (s *EthService) SendRawTransaction(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	rawTx := strings.TrimPrefix(args[0], "0x")
	txBytes, err := hex.DecodeString(rawTx)
	if err != nil {
		return nil, NewError(InvalidParams, "invalid hex string")
	}

	tx := new(types.Transaction)
	if err := rlp.DecodeBytes(txBytes, tx); err != nil {
		return nil, NewError(InvalidParams, "failed to decode transaction")
	}

	if err := s.client.SendTransaction(context.Background(), tx); err != nil {
		s.log.Error("Failed to send transaction", zap.Error(err))
		return nil, NewError(TransactionRejected, err.Error())
	}

	s.log.Info("Transaction sent", zap.String("hash", tx.Hash().Hex()))
	return tx.Hash().Hex(), nil
}

// GetTransactionByHash eth_getTransactionByHash
func (s *EthService) GetTransactionByHash(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	hash := common.HexToHash(args[0])
	tx, isPending, err := s.client.TransactionByHash(context.Background(), hash)
	if err != nil {
		if err == ethereum.NotFound {
			return nil, nil
		}
		return nil, NewError(ServerError, err.Error())
	}

	return s.formatTransaction(tx, isPending)
}

// GetTransactionReceipt eth_getTransactionReceipt
func (s *EthService) GetTransactionReceipt(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	hash := common.HexToHash(args[0])
	receipt, err := s.client.TransactionReceipt(context.Background(), hash)
	if err != nil {
		if err == ethereum.NotFound {
			return nil, nil
		}
		return nil, NewError(ServerError, err.Error())
	}

	return s.formatReceipt(receipt)
}

// Call eth_call
func (s *EthService) Call(params json.RawMessage) (interface{}, *Error) {
	var args []json.RawMessage
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	var callMsg struct {
		From     string `json:"from"`
		To       string `json:"to"`
		Gas      string `json:"gas"`
		GasPrice string `json:"gasPrice"`
		Value    string `json:"value"`
		Data     string `json:"data"`
	}

	if err := json.Unmarshal(args[0], &callMsg); err != nil {
		return nil, ErrInvalidParams
	}

	msg := ethereum.CallMsg{}

	if callMsg.From != "" {
		from := common.HexToAddress(callMsg.From)
		msg.From = from
	}

	if callMsg.To != "" {
		to := common.HexToAddress(callMsg.To)
		msg.To = &to
	}

	if callMsg.Gas != "" {
		gas, _ := hexutil.DecodeUint64(callMsg.Gas)
		msg.Gas = gas
	}

	if callMsg.GasPrice != "" {
		gasPrice, _ := hexutil.DecodeBig(callMsg.GasPrice)
		msg.GasPrice = gasPrice
	}

	if callMsg.Value != "" {
		value, _ := hexutil.DecodeBig(callMsg.Value)
		msg.Value = value
	}

	if callMsg.Data != "" {
		data, _ := hexutil.Decode(callMsg.Data)
		msg.Data = data
	}

	result, err := s.client.CallContract(context.Background(), msg, nil)
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	return hexutil.Encode(result), nil
}

// EstimateGas eth_estimateGas
func (s *EthService) EstimateGas(params json.RawMessage) (interface{}, *Error) {
	var args []json.RawMessage
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	var callMsg struct {
		From     string `json:"from"`
		To       string `json:"to"`
		Gas      string `json:"gas"`
		GasPrice string `json:"gasPrice"`
		Value    string `json:"value"`
		Data     string `json:"data"`
	}

	if err := json.Unmarshal(args[0], &callMsg); err != nil {
		return nil, ErrInvalidParams
	}

	msg := ethereum.CallMsg{}

	if callMsg.From != "" {
		from := common.HexToAddress(callMsg.From)
		msg.From = from
	}

	if callMsg.To != "" {
		to := common.HexToAddress(callMsg.To)
		msg.To = &to
	}

	if callMsg.GasPrice != "" {
		gasPrice, _ := hexutil.DecodeBig(callMsg.GasPrice)
		msg.GasPrice = gasPrice
	}

	if callMsg.Value != "" {
		value, _ := hexutil.DecodeBig(callMsg.Value)
		msg.Value = value
	}

	if callMsg.Data != "" {
		data, _ := hexutil.Decode(callMsg.Data)
		msg.Data = data
	}

	gas, err := s.client.EstimateGas(context.Background(), msg)
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	return hexutil.EncodeUint64(gas), nil
}

// ========== 区块方法 ==========

// GetBlockByNumber eth_getBlockByNumber
func (s *EthService) GetBlockByNumber(params json.RawMessage) (interface{}, *Error) {
	var args []interface{}
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 2 {
		return nil, ErrInvalidParams
	}

	blockNumStr, ok := args[0].(string)
	if !ok {
		return nil, ErrInvalidParams
	}

	fullTx, ok := args[1].(bool)
	if !ok {
		return nil, ErrInvalidParams
	}

	var blockNum *big.Int
	if blockNumStr == "latest" {
		blockNum = nil
	} else if blockNumStr == "pending" {
		blockNum = nil
	} else if blockNumStr == "earliest" {
		blockNum = big.NewInt(0)
	} else {
		num, err := hexutil.DecodeBig(blockNumStr)
		if err != nil {
			return nil, ErrInvalidParams
		}
		blockNum = num
	}

	block, err := s.client.BlockByNumber(context.Background(), blockNum)
	if err != nil {
		if err == ethereum.NotFound {
			return nil, nil
		}
		return nil, NewError(ServerError, err.Error())
	}

	return s.formatBlock(block, fullTx)
}

// GetBlockByHash eth_getBlockByHash
func (s *EthService) GetBlockByHash(params json.RawMessage) (interface{}, *Error) {
	var args []interface{}
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 2 {
		return nil, ErrInvalidParams
	}

	hashStr, ok := args[0].(string)
	if !ok {
		return nil, ErrInvalidParams
	}

	fullTx, ok := args[1].(bool)
	if !ok {
		return nil, ErrInvalidParams
	}

	hash := common.HexToHash(hashStr)
	block, err := s.client.BlockByHash(context.Background(), hash)
	if err != nil {
		if err == ethereum.NotFound {
			return nil, nil
		}
		return nil, NewError(ServerError, err.Error())
	}

	return s.formatBlock(block, fullTx)
}

// GetBlockTransactionCountByNumber eth_getBlockTransactionCountByNumber
func (s *EthService) GetBlockTransactionCountByNumber(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	blockNum := s.parseBlockNumberStr(args[0])

	block, err := s.client.BlockByNumber(context.Background(), blockNum)
	if err != nil {
		if err == ethereum.NotFound {
			return nil, nil
		}
		return nil, NewError(ServerError, err.Error())
	}

	return hexutil.EncodeUint64(uint64(len(block.Transactions()))), nil
}

// GetBlockTransactionCountByHash eth_getBlockTransactionCountByHash
func (s *EthService) GetBlockTransactionCountByHash(params json.RawMessage) (interface{}, *Error) {
	var args []string
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	hash := common.HexToHash(args[0])
	block, err := s.client.BlockByHash(context.Background(), hash)
	if err != nil {
		if err == ethereum.NotFound {
			return nil, nil
		}
		return nil, NewError(ServerError, err.Error())
	}

	return hexutil.EncodeUint64(uint64(len(block.Transactions()))), nil
}

// ========== 日志方法 ==========

// GetLogs eth_getLogs
func (s *EthService) GetLogs(params json.RawMessage) (interface{}, *Error) {
	var args []json.RawMessage
	if err := json.Unmarshal(params, &args); err != nil {
		return nil, ErrInvalidParams
	}

	if len(args) < 1 {
		return nil, ErrInvalidParams
	}

	var filter struct {
		FromBlock string        `json:"fromBlock"`
		ToBlock   string        `json:"toBlock"`
		Address   interface{}   `json:"address"`
		Topics    []interface{} `json:"topics"`
		BlockHash string        `json:"blockHash"`
	}

	if err := json.Unmarshal(args[0], &filter); err != nil {
		return nil, ErrInvalidParams
	}

	query := ethereum.FilterQuery{}

	if filter.BlockHash != "" {
		hash := common.HexToHash(filter.BlockHash)
		query.BlockHash = &hash
	} else {
		if filter.FromBlock != "" {
			query.FromBlock = s.parseBlockNumberStr(filter.FromBlock)
		}
		if filter.ToBlock != "" {
			query.ToBlock = s.parseBlockNumberStr(filter.ToBlock)
		}
	}

	// 解析地址
	if filter.Address != nil {
		switch addr := filter.Address.(type) {
		case string:
			query.Addresses = []common.Address{common.HexToAddress(addr)}
		case []interface{}:
			for _, a := range addr {
				if s, ok := a.(string); ok {
					query.Addresses = append(query.Addresses, common.HexToAddress(s))
				}
			}
		}
	}

	// 解析 topics
	for _, topic := range filter.Topics {
		switch t := topic.(type) {
		case nil:
			query.Topics = append(query.Topics, nil)
		case string:
			query.Topics = append(query.Topics, []common.Hash{common.HexToHash(t)})
		case []interface{}:
			var hashes []common.Hash
			for _, h := range t {
				if s, ok := h.(string); ok {
					hashes = append(hashes, common.HexToHash(s))
				}
			}
			query.Topics = append(query.Topics, hashes)
		}
	}

	logs, err := s.client.FilterLogs(context.Background(), query)
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	return s.formatLogs(logs)
}

// ========== 网络方法 ==========

// NetVersion net_version
func (s *EthService) NetVersion(params json.RawMessage) (interface{}, *Error) {
	return fmt.Sprintf("%d", s.chainID.Int64()), nil
}

// NetListening net_listening
func (s *EthService) NetListening(params json.RawMessage) (interface{}, *Error) {
	return true, nil
}

// Web3ClientVersion web3_clientVersion
func (s *EthService) Web3ClientVersion(params json.RawMessage) (interface{}, *Error) {
	return "evm-scanner-go/1.0.0", nil
}

// ========== 辅助方法 ==========

func (s *EthService) parseBlockNumber(args []string, index int) *big.Int {
	if len(args) <= index {
		return nil
	}
	return s.parseBlockNumberStr(args[index])
}

func (s *EthService) parseBlockNumberStr(blockStr string) *big.Int {
	switch blockStr {
	case "latest", "pending", "":
		return nil
	case "earliest":
		return big.NewInt(0)
	default:
		num, err := hexutil.DecodeBig(blockStr)
		if err != nil {
			return nil
		}
		return num
	}
}

func (s *EthService) formatTransaction(tx *types.Transaction, isPending bool) (interface{}, *Error) {
	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		return nil, NewError(ServerError, err.Error())
	}

	result := map[string]interface{}{
		"hash":     tx.Hash().Hex(),
		"nonce":    hexutil.EncodeUint64(tx.Nonce()),
		"from":     from.Hex(),
		"value":    hexutil.EncodeBig(tx.Value()),
		"gas":      hexutil.EncodeUint64(tx.Gas()),
		"gasPrice": hexutil.EncodeBig(tx.GasPrice()),
		"input":    hexutil.Encode(tx.Data()),
		"type":     hexutil.EncodeUint64(uint64(tx.Type())),
	}

	if tx.To() != nil {
		result["to"] = tx.To().Hex()
	} else {
		result["to"] = nil
	}

	if tx.ChainId() != nil {
		result["chainId"] = hexutil.EncodeBig(tx.ChainId())
	}

	// EIP-1559 字段
	if tx.Type() == types.DynamicFeeTxType {
		result["maxFeePerGas"] = hexutil.EncodeBig(tx.GasFeeCap())
		result["maxPriorityFeePerGas"] = hexutil.EncodeBig(tx.GasTipCap())
	}

	// 如果已确认，获取区块信息
	if !isPending {
		receipt, err := s.client.TransactionReceipt(context.Background(), tx.Hash())
		if err == nil && receipt != nil {
			result["blockHash"] = receipt.BlockHash.Hex()
			result["blockNumber"] = hexutil.EncodeBig(receipt.BlockNumber)
			result["transactionIndex"] = hexutil.EncodeUint64(uint64(receipt.TransactionIndex))
		}
	}

	return result, nil
}

func (s *EthService) formatReceipt(receipt *types.Receipt) (interface{}, *Error) {
	result := map[string]interface{}{
		"transactionHash":   receipt.TxHash.Hex(),
		"transactionIndex":  hexutil.EncodeUint64(uint64(receipt.TransactionIndex)),
		"blockHash":         receipt.BlockHash.Hex(),
		"blockNumber":       hexutil.EncodeBig(receipt.BlockNumber),
		"from":              "", // 需要从交易获取
		"cumulativeGasUsed": hexutil.EncodeUint64(receipt.CumulativeGasUsed),
		"gasUsed":           hexutil.EncodeUint64(receipt.GasUsed),
		"status":            hexutil.EncodeUint64(receipt.Status),
		"logsBloom":         hexutil.Encode(receipt.Bloom.Bytes()),
		"type":              hexutil.EncodeUint64(uint64(receipt.Type)),
	}

	if receipt.ContractAddress != (common.Address{}) {
		result["contractAddress"] = receipt.ContractAddress.Hex()
	} else {
		result["contractAddress"] = nil
	}

	// 获取交易以获取 from 和 to
	tx, _, err := s.client.TransactionByHash(context.Background(), receipt.TxHash)
	if err == nil {
		from, _ := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
		result["from"] = from.Hex()
		if tx.To() != nil {
			result["to"] = tx.To().Hex()
		} else {
			result["to"] = nil
		}
	}

	if receipt.EffectiveGasPrice != nil {
		result["effectiveGasPrice"] = hexutil.EncodeBig(receipt.EffectiveGasPrice)
	}

	// 格式化日志
	logs := make([]interface{}, len(receipt.Logs))
	for i, log := range receipt.Logs {
		logs[i] = s.formatLog(log)
	}
	result["logs"] = logs

	return result, nil
}

func (s *EthService) formatBlock(block *types.Block, fullTx bool) (interface{}, *Error) {
	result := map[string]interface{}{
		"number":           hexutil.EncodeBig(block.Number()),
		"hash":             block.Hash().Hex(),
		"parentHash":       block.ParentHash().Hex(),
		"nonce":            hexutil.EncodeUint64(block.Nonce()),
		"sha3Uncles":       block.UncleHash().Hex(),
		"logsBloom":        hexutil.Encode(block.Bloom().Bytes()),
		"transactionsRoot": block.TxHash().Hex(),
		"stateRoot":        block.Root().Hex(),
		"receiptsRoot":     block.ReceiptHash().Hex(),
		"miner":            block.Coinbase().Hex(),
		"difficulty":       hexutil.EncodeBig(block.Difficulty()),
		"totalDifficulty":  hexutil.EncodeBig(block.Difficulty()),
		"extraData":        hexutil.Encode(block.Extra()),
		"size":             hexutil.EncodeUint64(block.Size()),
		"gasLimit":         hexutil.EncodeUint64(block.GasLimit()),
		"gasUsed":          hexutil.EncodeUint64(block.GasUsed()),
		"timestamp":        hexutil.EncodeUint64(block.Time()),
	}

	if block.BaseFee() != nil {
		result["baseFeePerGas"] = hexutil.EncodeBig(block.BaseFee())
	}

	// 交易
	txs := block.Transactions()
	if fullTx {
		transactions := make([]interface{}, len(txs))
		for i, tx := range txs {
			formatted, _ := s.formatTransaction(tx, false)
			transactions[i] = formatted
		}
		result["transactions"] = transactions
	} else {
		hashes := make([]string, len(txs))
		for i, tx := range txs {
			hashes[i] = tx.Hash().Hex()
		}
		result["transactions"] = hashes
	}

	// Uncles
	uncles := make([]string, len(block.Uncles()))
	for i, uncle := range block.Uncles() {
		uncles[i] = uncle.Hash().Hex()
	}
	result["uncles"] = uncles

	return result, nil
}

func (s *EthService) formatLogs(logs []types.Log) (interface{}, *Error) {
	result := make([]interface{}, len(logs))
	for i, log := range logs {
		result[i] = s.formatLog(&log)
	}
	return result, nil
}

func (s *EthService) formatLog(log *types.Log) map[string]interface{} {
	topics := make([]string, len(log.Topics))
	for i, topic := range log.Topics {
		topics[i] = topic.Hex()
	}

	return map[string]interface{}{
		"address":          log.Address.Hex(),
		"topics":           topics,
		"data":             hexutil.Encode(log.Data),
		"blockNumber":      hexutil.EncodeUint64(log.BlockNumber),
		"transactionHash":  log.TxHash.Hex(),
		"transactionIndex": hexutil.EncodeUint64(uint64(log.TxIndex)),
		"blockHash":        log.BlockHash.Hex(),
		"logIndex":         hexutil.EncodeUint64(uint64(log.Index)),
		"removed":          log.Removed,
	}
}
