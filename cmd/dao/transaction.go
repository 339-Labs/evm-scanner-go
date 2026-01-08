package dao

import (
	"time"

	"evm-scanner-go/cmd/internal"

	"gorm.io/gorm"
)

// ChainTransaction 链上交易记录表
type ChainTransaction struct {
	ID              uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	TxHash          string    `gorm:"type:varchar(66);uniqueIndex;not null" json:"tx_hash"`
	BlockNumber     uint64    `gorm:"index;not null" json:"block_number"`
	BlockHash       string    `gorm:"type:varchar(66);not null" json:"block_hash"`
	BlockTime       time.Time `gorm:"index;not null" json:"block_time"`
	FromAddress     string    `gorm:"type:varchar(42);index;not null" json:"from_address"`
	ToAddress       string    `gorm:"type:varchar(42);index" json:"to_address"`
	Value           string    `gorm:"type:varchar(78);not null" json:"value"`
	GasPrice        string    `gorm:"type:varchar(78)" json:"gas_price"`
	GasUsed         uint64    `gorm:"not null" json:"gas_used"`
	Nonce           uint64    `gorm:"not null" json:"nonce"`
	TxIndex         uint      `gorm:"not null" json:"tx_index"`
	Input           string    `gorm:"type:text" json:"input"`
	Status          uint64    `gorm:"not null;default:1" json:"status"`
	ContractAddress string    `gorm:"type:varchar(42)" json:"contract_address"`
	TxType          int       `gorm:"index;not null" json:"tx_type"`
	ChainID         int64     `gorm:"index;not null" json:"chain_id"`
	ChainName       string    `gorm:"type:varchar(32);index;not null" json:"chain_name"`
	IsFromInternal  bool      `gorm:"not null" json:"is_from_internal"`
	IsToInternal    bool      `gorm:"not null" json:"is_to_internal"`
	CreatedAt       time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt       time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName 表名
func (ChainTransaction) TableName() string {
	return "chain_transactions"
}

// TokenTransfer Token转账记录表
type TokenTransfer struct {
	ID              uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	TxHash          string    `gorm:"type:varchar(66);index;not null" json:"tx_hash"`
	LogIndex        uint      `gorm:"not null" json:"log_index"`
	BlockNumber     uint64    `gorm:"index;not null" json:"block_number"`
	ContractAddress string    `gorm:"type:varchar(42);index;not null" json:"contract_address"`
	FromAddress     string    `gorm:"type:varchar(42);index;not null" json:"from_address"`
	ToAddress       string    `gorm:"type:varchar(42);index;not null" json:"to_address"`
	Value           string    `gorm:"type:varchar(78);not null" json:"value"`
	TokenSymbol     string    `gorm:"type:varchar(32)" json:"token_symbol"`
	TokenDecimals   uint8     `gorm:"not null;default:18" json:"token_decimals"`
	TxType          int       `gorm:"index;not null" json:"tx_type"`
	ChainID         int64     `gorm:"index;not null" json:"chain_id"`
	ChainName       string    `gorm:"type:varchar(32);index;not null" json:"chain_name"`
	IsFromInternal  bool      `gorm:"not null" json:"is_from_internal"`
	IsToInternal    bool      `gorm:"not null" json:"is_to_internal"`
	CreatedAt       time.Time `gorm:"autoCreateTime" json:"created_at"`
}

// TableName 表名
func (TokenTransfer) TableName() string {
	return "token_transfers"
}

// InternalTransfer 合约内部转账记录表（原生代币）
type InternalTransfer struct {
	ID             uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	TxHash         string    `gorm:"type:varchar(66);index;not null" json:"tx_hash"`
	TraceIndex     int       `gorm:"not null" json:"trace_index"`
	BlockNumber    uint64    `gorm:"index;not null" json:"block_number"`
	BlockHash      string    `gorm:"type:varchar(66);not null" json:"block_hash"`
	BlockTime      time.Time `gorm:"index;not null" json:"block_time"`
	TraceType      string    `gorm:"type:varchar(32);not null" json:"trace_type"`
	FromAddress    string    `gorm:"type:varchar(42);index;not null" json:"from_address"`
	ToAddress      string    `gorm:"type:varchar(42);index;not null" json:"to_address"`
	Value          string    `gorm:"type:varchar(78);not null" json:"value"`
	Gas            uint64    `gorm:"not null" json:"gas"`
	GasUsed        uint64    `gorm:"not null" json:"gas_used"`
	Depth          int       `gorm:"not null" json:"depth"`
	Error          string    `gorm:"type:text" json:"error"`
	TxType         int       `gorm:"index;not null" json:"tx_type"`
	ChainID        int64     `gorm:"index;not null" json:"chain_id"`
	ChainName      string    `gorm:"type:varchar(32);index;not null" json:"chain_name"`
	IsFromInternal bool      `gorm:"not null" json:"is_from_internal"`
	IsToInternal   bool      `gorm:"not null" json:"is_to_internal"`
	CreatedAt      time.Time `gorm:"autoCreateTime" json:"created_at"`
}

// TableName 表名
func (InternalTransfer) TableName() string {
	return "internal_transfers"
}

// ScanProgress 扫描进度表
type ScanProgress struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	ChainName string    `gorm:"type:varchar(32);uniqueIndex;not null" json:"chain_name"`
	ChainID   int64     `gorm:"not null" json:"chain_id"`
	LastBlock uint64    `gorm:"not null" json:"last_block"`
	UpdatedAt time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}

// TableName 表名
func (ScanProgress) TableName() string {
	return "scan_progress"
}

// TransactionDAO 交易数据访问对象
type TransactionDAO struct {
	db *gorm.DB
}

// NewTransactionDAO 创建TransactionDAO
func NewTransactionDAO() *TransactionDAO {
	return &TransactionDAO{db: DB}
}

// SaveTransaction 保存交易记录
func (d *TransactionDAO) SaveTransaction(tx *internal.Transaction) error {
	record := &ChainTransaction{
		TxHash:          tx.TxHash,
		BlockNumber:     tx.BlockNumber,
		BlockHash:       tx.BlockHash,
		BlockTime:       tx.BlockTime,
		FromAddress:     tx.From,
		ToAddress:       tx.To,
		Value:           tx.ValueStr,
		GasPrice:        tx.GasPrice.String(),
		GasUsed:         tx.GasUsed,
		Nonce:           tx.Nonce,
		TxIndex:         tx.TxIndex,
		Input:           tx.Input,
		Status:          tx.Status,
		ContractAddress: tx.ContractAddress,
		TxType:          int(tx.TxType),
		ChainID:         tx.ChainID,
		ChainName:       tx.ChainName,
		IsFromInternal:  tx.IsFromInternal,
		IsToInternal:    tx.IsToInternal,
	}

	// 使用 ON DUPLICATE KEY UPDATE 避免重复插入
	return d.db.Clauses().Create(record).Error
}

// SaveTokenTransfer 保存Token转账记录
func (d *TransactionDAO) SaveTokenTransfer(transfer *internal.TokenTransfer, chainID int64, chainName string) error {
	record := &TokenTransfer{
		TxHash:          transfer.TxHash,
		LogIndex:        transfer.LogIndex,
		BlockNumber:     transfer.BlockNumber,
		ContractAddress: transfer.ContractAddress,
		FromAddress:     transfer.From,
		ToAddress:       transfer.To,
		Value:           transfer.ValueStr,
		TokenSymbol:     transfer.TokenSymbol,
		TokenDecimals:   transfer.TokenDecimals,
		TxType:          int(transfer.TxType),
		ChainID:         chainID,
		ChainName:       chainName,
		IsFromInternal:  transfer.IsFromInternal,
		IsToInternal:    transfer.IsToInternal,
	}

	return d.db.Create(record).Error
}

// BatchSaveTransactions 批量保存交易记录
func (d *TransactionDAO) BatchSaveTransactions(txs []*internal.Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	records := make([]*ChainTransaction, 0, len(txs))
	for _, tx := range txs {
		records = append(records, &ChainTransaction{
			TxHash:          tx.TxHash,
			BlockNumber:     tx.BlockNumber,
			BlockHash:       tx.BlockHash,
			BlockTime:       tx.BlockTime,
			FromAddress:     tx.From,
			ToAddress:       tx.To,
			Value:           tx.ValueStr,
			GasPrice:        tx.GasPrice.String(),
			GasUsed:         tx.GasUsed,
			Nonce:           tx.Nonce,
			TxIndex:         tx.TxIndex,
			Input:           tx.Input,
			Status:          tx.Status,
			ContractAddress: tx.ContractAddress,
			TxType:          int(tx.TxType),
			ChainID:         tx.ChainID,
			ChainName:       tx.ChainName,
			IsFromInternal:  tx.IsFromInternal,
			IsToInternal:    tx.IsToInternal,
		})
	}

	return d.db.CreateInBatches(records, 100).Error
}

// GetLastScannedBlock 获取最后扫描的区块
func (d *TransactionDAO) GetLastScannedBlock(chainName string) (uint64, error) {
	var progress ScanProgress
	err := d.db.Where("chain_name = ?", chainName).First(&progress).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return progress.LastBlock, nil
}

// UpdateScanProgress 更新扫描进度
func (d *TransactionDAO) UpdateScanProgress(chainName string, chainID int64, blockNumber uint64) error {
	progress := ScanProgress{
		ChainName: chainName,
		ChainID:   chainID,
		LastBlock: blockNumber,
	}

	return d.db.Where("chain_name = ?", chainName).
		Assign(progress).
		FirstOrCreate(&progress).Error
}

// GetTransactionByHash 根据Hash获取交易
func (d *TransactionDAO) GetTransactionByHash(txHash string) (*ChainTransaction, error) {
	var tx ChainTransaction
	err := d.db.Where("tx_hash = ?", txHash).First(&tx).Error
	if err != nil {
		return nil, err
	}
	return &tx, nil
}

// GetTransactionsByAddress 获取地址相关的交易
func (d *TransactionDAO) GetTransactionsByAddress(address string, limit, offset int) ([]*ChainTransaction, error) {
	var txs []*ChainTransaction
	err := d.db.Where("from_address = ? OR to_address = ?", address, address).
		Order("block_number DESC").
		Limit(limit).
		Offset(offset).
		Find(&txs).Error
	return txs, err
}

// GetDepositTransactions 获取充值交易
func (d *TransactionDAO) GetDepositTransactions(chainName string, limit, offset int) ([]*ChainTransaction, error) {
	var txs []*ChainTransaction
	err := d.db.Where("chain_name = ? AND tx_type = ?", chainName, internal.TransactionTypeDeposit).
		Order("block_number DESC").
		Limit(limit).
		Offset(offset).
		Find(&txs).Error
	return txs, err
}

// GetWithdrawTransactions 获取提现交易
func (d *TransactionDAO) GetWithdrawTransactions(chainName string, limit, offset int) ([]*ChainTransaction, error) {
	var txs []*ChainTransaction
	err := d.db.Where("chain_name = ? AND tx_type = ?", chainName, internal.TransactionTypeWithdraw).
		Order("block_number DESC").
		Limit(limit).
		Offset(offset).
		Find(&txs).Error
	return txs, err
}

// SaveInternalTransfer 保存合约内部转账记录
func (d *TransactionDAO) SaveInternalTransfer(transfer *internal.InternalTransfer) error {
	record := &InternalTransfer{
		TxHash:         transfer.TxHash,
		TraceIndex:     transfer.TraceIndex,
		BlockNumber:    transfer.BlockNumber,
		BlockHash:      transfer.BlockHash,
		BlockTime:      transfer.BlockTime,
		TraceType:      transfer.TraceType,
		FromAddress:    transfer.From,
		ToAddress:      transfer.To,
		Value:          transfer.ValueStr,
		Gas:            transfer.Gas,
		GasUsed:        transfer.GasUsed,
		Depth:          transfer.Depth,
		Error:          transfer.Error,
		TxType:         int(transfer.TxType),
		ChainID:        transfer.ChainID,
		ChainName:      transfer.ChainName,
		IsFromInternal: transfer.IsFromInternal,
		IsToInternal:   transfer.IsToInternal,
	}

	return d.db.Create(record).Error
}

// GetInternalTransfersByTxHash 获取交易的内部转账
func (d *TransactionDAO) GetInternalTransfersByTxHash(txHash string) ([]*InternalTransfer, error) {
	var transfers []*InternalTransfer
	err := d.db.Where("tx_hash = ?", txHash).
		Order("trace_index ASC").
		Find(&transfers).Error
	return transfers, err
}

// GetInternalTransfersByAddress 获取地址相关的内部转账
func (d *TransactionDAO) GetInternalTransfersByAddress(address string, limit, offset int) ([]*InternalTransfer, error) {
	var transfers []*InternalTransfer
	err := d.db.Where("from_address = ? OR to_address = ?", address, address).
		Order("block_number DESC").
		Limit(limit).
		Offset(offset).
		Find(&transfers).Error
	return transfers, err
}
