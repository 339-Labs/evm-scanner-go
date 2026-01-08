package internal

import (
	"math/big"
	"time"
)

// TransactionType 交易类型
type TransactionType int

const (
	TransactionTypeUnknown  TransactionType = 0 // 未知类型
	TransactionTypeDeposit  TransactionType = 1 // 充值（外部->内部）
	TransactionTypeWithdraw TransactionType = 2 // 提现（内部->外部）
	TransactionTypeInternal TransactionType = 3 // 内部转账（内部->内部）
)

func (t TransactionType) String() string {
	switch t {
	case TransactionTypeDeposit:
		return "deposit"
	case TransactionTypeWithdraw:
		return "withdraw"
	case TransactionTypeInternal:
		return "internal"
	default:
		return "unknown"
	}
}

// Transaction 解析后的交易信息
type Transaction struct {
	TxHash          string          `json:"tx_hash"`
	BlockNumber     uint64          `json:"block_number"`
	BlockHash       string          `json:"block_hash"`
	BlockTime       time.Time       `json:"block_time"`
	From            string          `json:"from"`
	To              string          `json:"to"`
	Value           *big.Int        `json:"value"`
	ValueStr        string          `json:"value_str"`
	GasPrice        *big.Int        `json:"gas_price"`
	GasUsed         uint64          `json:"gas_used"`
	Nonce           uint64          `json:"nonce"`
	TxIndex         uint            `json:"tx_index"`
	Input           string          `json:"input"`
	Status          uint64          `json:"status"` // 1=成功, 0=失败
	ContractAddress string          `json:"contract_address,omitempty"`
	TxType          TransactionType `json:"tx_type"`
	ChainID         int64           `json:"chain_id"`
	ChainName       string          `json:"chain_name"`
	IsFromInternal  bool            `json:"is_from_internal"`
	IsToInternal    bool            `json:"is_to_internal"`
}

// TokenTransfer ERC20 Token转账信息
type TokenTransfer struct {
	TxHash          string          `json:"tx_hash"`
	LogIndex        uint            `json:"log_index"`
	BlockNumber     uint64          `json:"block_number"`
	ContractAddress string          `json:"contract_address"`
	From            string          `json:"from"`
	To              string          `json:"to"`
	Value           *big.Int        `json:"value"`
	ValueStr        string          `json:"value_str"`
	TokenSymbol     string          `json:"token_symbol,omitempty"`
	TokenDecimals   uint8           `json:"token_decimals,omitempty"`
	TxType          TransactionType `json:"tx_type"`
	IsFromInternal  bool            `json:"is_from_internal"`
	IsToInternal    bool            `json:"is_to_internal"`
}

// InternalTransfer 合约内部转账（原生代币）
// 通过 debug_traceTransaction 或 trace_transaction 获取
type InternalTransfer struct {
	TxHash         string          `json:"tx_hash"`
	BlockNumber    uint64          `json:"block_number"`
	BlockHash      string          `json:"block_hash"`
	BlockTime      time.Time       `json:"block_time"`
	TraceIndex     int             `json:"trace_index"` // trace 中的索引
	TraceType      string          `json:"trace_type"`  // CALL, CALLCODE, DELEGATECALL, STATICCALL, CREATE, CREATE2, SELFDESTRUCT
	From           string          `json:"from"`
	To             string          `json:"to"`
	Value          *big.Int        `json:"value"`
	ValueStr       string          `json:"value_str"`
	Gas            uint64          `json:"gas"`
	GasUsed        uint64          `json:"gas_used"`
	Input          string          `json:"input"`
	Output         string          `json:"output"`
	Error          string          `json:"error,omitempty"` // 调用失败的错误信息
	Depth          int             `json:"depth"`           // 调用深度
	TxType         TransactionType `json:"tx_type"`
	ChainID        int64           `json:"chain_id"`
	ChainName      string          `json:"chain_name"`
	IsFromInternal bool            `json:"is_from_internal"`
	IsToInternal   bool            `json:"is_to_internal"`
}

// TraceCallType trace 调用类型
type TraceCallType string

const (
	TraceCallTypeCall         TraceCallType = "CALL"
	TraceCallTypeCallCode     TraceCallType = "CALLCODE"
	TraceCallTypeDelegateCall TraceCallType = "DELEGATECALL"
	TraceCallTypeStaticCall   TraceCallType = "STATICCALL"
	TraceCallTypeCreate       TraceCallType = "CREATE"
	TraceCallTypeCreate2      TraceCallType = "CREATE2"
	TraceCallTypeSelfDestruct TraceCallType = "SELFDESTRUCT"
)

// BlockInfo 区块信息
type BlockInfo struct {
	BlockNumber uint64    `json:"block_number"`
	BlockHash   string    `json:"block_hash"`
	ParentHash  string    `json:"parent_hash"`
	Timestamp   time.Time `json:"timestamp"`
	TxCount     int       `json:"tx_count"`
	GasUsed     uint64    `json:"gas_used"`
	GasLimit    uint64    `json:"gas_limit"`
	BaseFee     *big.Int  `json:"base_fee,omitempty"`
}

// ScanProgress 扫描进度
type ScanProgress struct {
	ChainName   string    `json:"chain_name"`
	ChainID     int64     `json:"chain_id"`
	LastBlock   uint64    `json:"last_block"`
	LatestBlock uint64    `json:"latest_block"`
	UpdateTime  time.Time `json:"update_time"`
}

// MQMessage RocketMQ消息结构
type MQMessage struct {
	Type      string      `json:"type"` // "transaction" 或 "token_transfer"
	ChainName string      `json:"chain_name"`
	ChainID   int64       `json:"chain_id"`
	Data      interface{} `json:"data"`
	Timestamp int64       `json:"timestamp"`
}
