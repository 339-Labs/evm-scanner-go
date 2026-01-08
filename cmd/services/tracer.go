package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"evm-scanner-go/cmd/config"
	"evm-scanner-go/cmd/internal"

	"go.uber.org/zap"
)

// Tracer 交易追踪器，用于解析合约内部转账
type Tracer struct {
	rpcURL     string
	httpClient *http.Client
	log        *zap.Logger
	cfg        *config.ChainConfig
	traceType  string // "debug" (Geth) 或 "trace" (Parity/Erigon)
	mu         sync.RWMutex
}

// NewTracer 创建追踪器
func NewTracer(cfg *config.ChainConfig, log *zap.Logger) *Tracer {
	return &Tracer{
		rpcURL: cfg.RPCURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		log:       log,
		cfg:       cfg,
		traceType: cfg.TraceType, // 默认使用 debug (Geth)
	}
}

// SetTraceType 设置 trace 类型
func (t *Tracer) SetTraceType(traceType string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.traceType = traceType
}

// ========== JSON-RPC 请求结构 ==========

type rpcRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
	ID      int             `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ========== Geth debug_traceTransaction 响应结构 ==========

// GethTraceResult Geth trace 结果 (使用 callTracer)
type GethTraceResult struct {
	Type    string            `json:"type"`
	From    string            `json:"from"`
	To      string            `json:"to"`
	Value   string            `json:"value"`
	Gas     string            `json:"gas"`
	GasUsed string            `json:"gasUsed"`
	Input   string            `json:"input"`
	Output  string            `json:"output"`
	Error   string            `json:"error"`
	Calls   []GethTraceResult `json:"calls"`
}

// ========== Parity/Erigon trace_transaction 响应结构 ==========

// ParityTraceResult Parity trace 结果
type ParityTraceResult struct {
	Action       ParityTraceAction `json:"action"`
	Result       *ParityTraceRes   `json:"result"`
	Error        string            `json:"error"`
	Subtraces    int               `json:"subtraces"`
	TraceAddress []int             `json:"traceAddress"`
	Type         string            `json:"type"` // "call", "create", "suicide"
}

type ParityTraceAction struct {
	CallType      string `json:"callType"` // "call", "callcode", "delegatecall", "staticcall"
	From          string `json:"from"`
	To            string `json:"to"`
	Value         string `json:"value"`
	Gas           string `json:"gas"`
	Input         string `json:"input"`
	Init          string `json:"init"`          // for create
	Address       string `json:"address"`       // for suicide
	RefundAddress string `json:"refundAddress"` // for suicide
	Balance       string `json:"balance"`       // for suicide
}

type ParityTraceRes struct {
	GasUsed string `json:"gasUsed"`
	Output  string `json:"output"`
	Address string `json:"address"` // for create
	Code    string `json:"code"`    // for create
}

// ========== 追踪方法 ==========

// TraceTransaction 追踪单个交易，返回内部转账列表
func (t *Tracer) TraceTransaction(ctx context.Context, txHash string, blockNumber uint64, blockHash string, blockTime time.Time) ([]*internal.InternalTransfer, error) {
	t.mu.RLock()
	traceType := t.traceType
	t.mu.RUnlock()

	if traceType == "trace" {
		return t.traceTransactionParity(ctx, txHash, blockNumber, blockHash, blockTime)
	}
	return t.traceTransactionGeth(ctx, txHash, blockNumber, blockHash, blockTime)
}

// traceTransactionGeth 使用 Geth debug_traceTransaction
func (t *Tracer) traceTransactionGeth(ctx context.Context, txHash string, blockNumber uint64, blockHash string, blockTime time.Time) ([]*internal.InternalTransfer, error) {
	// 使用 callTracer 获取内部调用
	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  "debug_traceTransaction",
		Params: []interface{}{
			txHash,
			map[string]interface{}{
				"tracer": "callTracer",
				"tracerConfig": map[string]interface{}{
					"onlyTopCall": false,
					"withLog":     false,
				},
			},
		},
		ID: 1,
	}

	resp, err := t.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("trace request failed: %w", err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("trace error: %s", resp.Error.Message)
	}

	var traceResult GethTraceResult
	if err := json.Unmarshal(resp.Result, &traceResult); err != nil {
		return nil, fmt.Errorf("failed to parse trace result: %w", err)
	}

	// 解析内部转账
	transfers := make([]*internal.InternalTransfer, 0)
	t.extractGethInternalTransfers(&traceResult, txHash, blockNumber, blockHash, blockTime, 0, &transfers, 0)

	return transfers, nil
}

// extractGethInternalTransfers 递归提取 Geth trace 中的内部转账
func (t *Tracer) extractGethInternalTransfers(
	trace *GethTraceResult,
	txHash string,
	blockNumber uint64,
	blockHash string,
	blockTime time.Time,
	depth int,
	transfers *[]*internal.InternalTransfer,
	index int,
) int {
	// 解析 value
	value := t.parseHexBigInt(trace.Value)

	// 只有有 value 的 CALL 类型才是内部转账
	// DELEGATECALL 和 STATICCALL 不转移原生代币
	if value != nil && value.Sign() > 0 && (trace.Type == "CALL" || trace.Type == "CREATE" || trace.Type == "CREATE2" || trace.Type == "SELFDESTRUCT") {
		transfer := &internal.InternalTransfer{
			TxHash:      txHash,
			BlockNumber: blockNumber,
			BlockHash:   blockHash,
			BlockTime:   blockTime,
			TraceIndex:  index,
			TraceType:   trace.Type,
			From:        strings.ToLower(trace.From),
			To:          strings.ToLower(trace.To),
			Value:       value,
			ValueStr:    value.String(),
			Gas:         t.parseHexUint64(trace.Gas),
			GasUsed:     t.parseHexUint64(trace.GasUsed),
			Input:       trace.Input,
			Output:      trace.Output,
			Error:       trace.Error,
			Depth:       depth,
			ChainID:     t.cfg.ChainID,
			ChainName:   t.cfg.Name,
		}
		*transfers = append(*transfers, transfer)
	}

	index++

	// 递归处理子调用
	for _, call := range trace.Calls {
		index = t.extractGethInternalTransfers(&call, txHash, blockNumber, blockHash, blockTime, depth+1, transfers, index)
	}

	return index
}

// traceTransactionParity 使用 Parity/Erigon trace_transaction
func (t *Tracer) traceTransactionParity(ctx context.Context, txHash string, blockNumber uint64, blockHash string, blockTime time.Time) ([]*internal.InternalTransfer, error) {
	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  "trace_transaction",
		Params:  []interface{}{txHash},
		ID:      1,
	}

	resp, err := t.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("trace request failed: %w", err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("trace error: %s", resp.Error.Message)
	}

	var traces []ParityTraceResult
	if err := json.Unmarshal(resp.Result, &traces); err != nil {
		return nil, fmt.Errorf("failed to parse trace result: %w", err)
	}

	// 解析内部转账
	transfers := make([]*internal.InternalTransfer, 0)

	for i, trace := range traces {
		transfer := t.parseParityTrace(&trace, txHash, blockNumber, blockHash, blockTime, i)
		if transfer != nil {
			transfers = append(transfers, transfer)
		}
	}

	return transfers, nil
}

// parseParityTrace 解析 Parity trace
func (t *Tracer) parseParityTrace(trace *ParityTraceResult, txHash string, blockNumber uint64, blockHash string, blockTime time.Time, index int) *internal.InternalTransfer {
	var value *big.Int
	var from, to string
	var traceType string

	switch trace.Type {
	case "call":
		value = t.parseHexBigInt(trace.Action.Value)
		from = trace.Action.From
		to = trace.Action.To
		traceType = strings.ToUpper(trace.Action.CallType)

	case "create":
		value = t.parseHexBigInt(trace.Action.Value)
		from = trace.Action.From
		if trace.Result != nil {
			to = trace.Result.Address
		}
		traceType = "CREATE"

	case "suicide":
		value = t.parseHexBigInt(trace.Action.Balance)
		from = trace.Action.Address
		to = trace.Action.RefundAddress
		traceType = "SELFDESTRUCT"

	default:
		return nil
	}

	// 只返回有 value 的转账
	if value == nil || value.Sign() <= 0 {
		return nil
	}

	// DELEGATECALL 和 STATICCALL 不转移原生代币
	if traceType == "DELEGATECALL" || traceType == "STATICCALL" {
		return nil
	}

	var gasUsed uint64
	if trace.Result != nil {
		gasUsed = t.parseHexUint64(trace.Result.GasUsed)
	}

	return &internal.InternalTransfer{
		TxHash:      txHash,
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
		BlockTime:   blockTime,
		TraceIndex:  index,
		TraceType:   traceType,
		From:        strings.ToLower(from),
		To:          strings.ToLower(to),
		Value:       value,
		ValueStr:    value.String(),
		Gas:         t.parseHexUint64(trace.Action.Gas),
		GasUsed:     gasUsed,
		Input:       trace.Action.Input,
		Error:       trace.Error,
		Depth:       len(trace.TraceAddress),
		ChainID:     t.cfg.ChainID,
		ChainName:   t.cfg.Name,
	}
}

// TraceBlock 追踪整个区块的所有交易
func (t *Tracer) TraceBlock(ctx context.Context, blockNumber uint64, blockHash string, blockTime time.Time) (map[string][]*internal.InternalTransfer, error) {
	t.mu.RLock()
	traceType := t.traceType
	t.mu.RUnlock()

	if traceType == "trace" {
		return t.traceBlockParity(ctx, blockNumber, blockHash, blockTime)
	}
	return t.traceBlockGeth(ctx, blockNumber, blockHash, blockTime)
}

// traceBlockGeth 使用 debug_traceBlockByNumber
func (t *Tracer) traceBlockGeth(ctx context.Context, blockNumber uint64, blockHash string, blockTime time.Time) (map[string][]*internal.InternalTransfer, error) {
	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  "debug_traceBlockByNumber",
		Params: []interface{}{
			fmt.Sprintf("0x%x", blockNumber),
			map[string]interface{}{
				"tracer": "callTracer",
				"tracerConfig": map[string]interface{}{
					"onlyTopCall": false,
					"withLog":     false,
				},
			},
		},
		ID: 1,
	}

	resp, err := t.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("trace block request failed: %w", err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("trace block error: %s", resp.Error.Message)
	}

	var blockTraces []struct {
		TxHash string          `json:"txHash"`
		Result GethTraceResult `json:"result"`
	}

	if err := json.Unmarshal(resp.Result, &blockTraces); err != nil {
		return nil, fmt.Errorf("failed to parse block trace result: %w", err)
	}

	result := make(map[string][]*internal.InternalTransfer)
	for _, bt := range blockTraces {
		transfers := make([]*internal.InternalTransfer, 0)
		t.extractGethInternalTransfers(&bt.Result, bt.TxHash, blockNumber, blockHash, blockTime, 0, &transfers, 0)
		if len(transfers) > 0 {
			result[bt.TxHash] = transfers
		}
	}

	return result, nil
}

// traceBlockParity 使用 trace_block
func (t *Tracer) traceBlockParity(ctx context.Context, blockNumber uint64, blockHash string, blockTime time.Time) (map[string][]*internal.InternalTransfer, error) {
	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  "trace_block",
		Params:  []interface{}{fmt.Sprintf("0x%x", blockNumber)},
		ID:      1,
	}

	resp, err := t.doRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("trace block request failed: %w", err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("trace block error: %s", resp.Error.Message)
	}

	var traces []struct {
		ParityTraceResult
		TransactionHash string `json:"transactionHash"`
	}

	if err := json.Unmarshal(resp.Result, &traces); err != nil {
		return nil, fmt.Errorf("failed to parse block trace result: %w", err)
	}

	result := make(map[string][]*internal.InternalTransfer)
	indexMap := make(map[string]int)

	for _, trace := range traces {
		if trace.TransactionHash == "" {
			continue // 跳过区块奖励等
		}

		idx := indexMap[trace.TransactionHash]
		indexMap[trace.TransactionHash]++

		transfer := t.parseParityTrace(&trace.ParityTraceResult, trace.TransactionHash, blockNumber, blockHash, blockTime, idx)
		if transfer != nil {
			result[trace.TransactionHash] = append(result[trace.TransactionHash], transfer)
		}
	}

	return result, nil
}

// ========== 辅助方法 ==========

func (t *Tracer) doRequest(ctx context.Context, req rpcRequest) (*rpcResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", t.rpcURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := t.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	var resp rpcResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (t *Tracer) parseHexBigInt(hex string) *big.Int {
	if hex == "" || hex == "0x" || hex == "0x0" {
		return big.NewInt(0)
	}
	hex = strings.TrimPrefix(hex, "0x")
	value, _ := new(big.Int).SetString(hex, 16)
	if value == nil {
		return big.NewInt(0)
	}
	return value
}

func (t *Tracer) parseHexUint64(hex string) uint64 {
	if hex == "" || hex == "0x" {
		return 0
	}
	hex = strings.TrimPrefix(hex, "0x")
	var value uint64
	fmt.Sscanf(hex, "%x", &value)
	return value
}

// CheckTraceSupport 检查节点是否支持 trace
func (t *Tracer) CheckTraceSupport(ctx context.Context) (bool, string, error) {
	// 尝试 debug_traceTransaction (Geth)
	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  "debug_traceTransaction",
		Params: []interface{}{
			"0x0000000000000000000000000000000000000000000000000000000000000000",
			map[string]interface{}{"tracer": "callTracer"},
		},
		ID: 1,
	}

	resp, err := t.doRequest(ctx, req)
	if err == nil && resp.Error != nil {
		// 如果返回的是 "transaction not found" 而不是 "method not found"，说明支持
		if !strings.Contains(resp.Error.Message, "method") {
			t.log.Info("Node supports debug_traceTransaction (Geth)")
			return true, "debug", nil
		}
	}

	// 尝试 trace_transaction (Parity/Erigon)
	req.Method = "trace_transaction"
	req.Params = []interface{}{"0x0000000000000000000000000000000000000000000000000000000000000000"}

	resp, err = t.doRequest(ctx, req)
	if err == nil && resp.Error != nil {
		if !strings.Contains(resp.Error.Message, "method") {
			t.log.Info("Node supports trace_transaction (Parity/Erigon)")
			return true, "trace", nil
		}
	}

	t.log.Warn("Node does not support trace APIs")
	return false, "", nil
}
