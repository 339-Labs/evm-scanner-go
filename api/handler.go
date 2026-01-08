package api

import (
	"encoding/json"
	"math/big"
	"net/http"
	"strings"

	"evm-scanner-go/cmd/services"

	"go.uber.org/zap"
)

// Handler API 处理器
type Handler struct {
	rpc     *services.RPCService
	scanner *services.Scanner
	log     *zap.Logger
}

// NewHandler 创建 Handler
func NewHandler(rpc *services.RPCService, scanner *services.Scanner, log *zap.Logger) *Handler {
	return &Handler{
		rpc:     rpc,
		scanner: scanner,
		log:     log,
	}
}

// Response 统一响应结构
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// 响应辅助函数
func (h *Handler) jsonResponse(w http.ResponseWriter, code int, message string, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	resp := Response{
		Code:    code,
		Message: message,
		Data:    data,
	}

	if code != 0 {
		w.WriteHeader(http.StatusBadRequest)
	}

	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) success(w http.ResponseWriter, data interface{}) {
	h.jsonResponse(w, 0, "success", data)
}

func (h *Handler) error(w http.ResponseWriter, code int, message string) {
	h.jsonResponse(w, code, message, nil)
}

// SendRawTransactionRequest 发送交易请求
type SendRawTransactionRequest struct {
	RawTx string `json:"raw_tx"` // 签名后的交易数据（十六进制）
}

// SendRawTransaction 发送原始交易上链
// POST /api/v1/transaction/send
func (h *Handler) SendRawTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.error(w, 405, "method not allowed")
		return
	}

	var req SendRawTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, 400, "invalid request body")
		return
	}

	if req.RawTx == "" {
		h.error(w, 400, "raw_tx is required")
		return
	}

	txHash, err := h.rpc.SendRawTransaction(r.Context(), req.RawTx)
	if err != nil {
		h.log.Error("Failed to send transaction", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	h.success(w, map[string]string{
		"tx_hash": txHash,
	})
}

// GetTransactionReceipt 获取交易回执
// GET /api/v1/transaction/receipt/{txHash}
func (h *Handler) GetTransactionReceipt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.error(w, 405, "method not allowed")
		return
	}

	txHash := strings.TrimPrefix(r.URL.Path, "/api/v1/transaction/receipt/")
	if txHash == "" {
		h.error(w, 400, "tx_hash is required")
		return
	}

	receipt, err := h.rpc.GetTransactionReceipt(r.Context(), txHash)
	if err != nil {
		h.log.Error("Failed to get receipt", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	if receipt == nil {
		h.error(w, 404, "transaction not found or not confirmed")
		return
	}

	h.success(w, receipt)
}

// GetTransactionDetail 获取交易详情
// GET /api/v1/transaction/detail/{txHash}
func (h *Handler) GetTransactionDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.error(w, 405, "method not allowed")
		return
	}

	txHash := strings.TrimPrefix(r.URL.Path, "/api/v1/transaction/detail/")
	if txHash == "" {
		h.error(w, 400, "tx_hash is required")
		return
	}

	detail, err := h.rpc.GetTransactionByHash(r.Context(), txHash)
	if err != nil {
		h.log.Error("Failed to get transaction", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	if detail == nil {
		h.error(w, 404, "transaction not found")
		return
	}

	h.success(w, detail)
}

// GetTransactionStatus 获取交易状态
// GET /api/v1/transaction/status/{txHash}
func (h *Handler) GetTransactionStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.error(w, 405, "method not allowed")
		return
	}

	txHash := strings.TrimPrefix(r.URL.Path, "/api/v1/transaction/status/")
	if txHash == "" {
		h.error(w, 400, "tx_hash is required")
		return
	}

	status, err := h.rpc.GetTransactionStatus(r.Context(), txHash)
	if err != nil {
		h.log.Error("Failed to get transaction status", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	h.success(w, map[string]string{
		"tx_hash": txHash,
		"status":  status,
	})
}

// GetBlockNumber 获取最新区块号
// GET /api/v1/chain/block-number
func (h *Handler) GetBlockNumber(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.error(w, 405, "method not allowed")
		return
	}

	blockNumber, err := h.rpc.GetBlockNumber(r.Context())
	if err != nil {
		h.log.Error("Failed to get block number", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"block_number": blockNumber,
	})
}

// GetBalance 获取账户余额
// GET /api/v1/account/balance/{address}
func (h *Handler) GetBalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.error(w, 405, "method not allowed")
		return
	}

	address := strings.TrimPrefix(r.URL.Path, "/api/v1/account/balance/")
	if address == "" {
		h.error(w, 400, "address is required")
		return
	}

	balance, err := h.rpc.GetBalance(r.Context(), address)
	if err != nil {
		h.log.Error("Failed to get balance", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	h.success(w, map[string]string{
		"address": address,
		"balance": balance.String(),
	})
}

// GetNonce 获取账户 nonce
// GET /api/v1/account/nonce/{address}
func (h *Handler) GetNonce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.error(w, 405, "method not allowed")
		return
	}

	address := strings.TrimPrefix(r.URL.Path, "/api/v1/account/nonce/")
	if address == "" {
		h.error(w, 400, "address is required")
		return
	}

	nonce, err := h.rpc.GetNonce(r.Context(), address)
	if err != nil {
		h.log.Error("Failed to get nonce", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	h.success(w, map[string]interface{}{
		"address": address,
		"nonce":   nonce,
	})
}

// GetGasPrice 获取 gas 价格
// GET /api/v1/chain/gas-price
func (h *Handler) GetGasPrice(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.error(w, 405, "method not allowed")
		return
	}

	gasPrice, err := h.rpc.GetGasPrice(r.Context())
	if err != nil {
		h.log.Error("Failed to get gas price", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	h.success(w, map[string]string{
		"gas_price": gasPrice.String(),
	})
}

// EstimateGasRequest 估算 gas 请求
type EstimateGasRequest struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Value string `json:"value"` // wei 为单位
	Data  string `json:"data"`  // 十六进制，可选
}

// EstimateGas 估算 gas
// POST /api/v1/chain/estimate-gas
func (h *Handler) EstimateGas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.error(w, 405, "method not allowed")
		return
	}

	var req EstimateGasRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.error(w, 400, "invalid request body")
		return
	}

	if req.From == "" || req.To == "" {
		h.error(w, 400, "from and to are required")
		return
	}

	value := big.NewInt(0)
	if req.Value != "" {
		var ok bool
		value, ok = new(big.Int).SetString(req.Value, 10)
		if !ok {
			h.error(w, 400, "invalid value")
			return
		}
	}

	var data []byte
	if req.Data != "" {
		dataStr := strings.TrimPrefix(req.Data, "0x")
		data, _ = hexDecode(dataStr)
	}

	gas, err := h.rpc.EstimateGas(r.Context(), req.From, req.To, value, data)
	if err != nil {
		h.log.Error("Failed to estimate gas", zap.Error(err))
		h.error(w, 500, err.Error())
		return
	}

	h.success(w, map[string]uint64{
		"gas": gas,
	})
}

// hexDecode 解码十六进制字符串
func hexDecode(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	if len(s)%2 != 0 {
		s = "0" + s
	}
	result := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		var val byte
		for j := 0; j < 2; j++ {
			c := s[i+j]
			switch {
			case '0' <= c && c <= '9':
				val = val*16 + (c - '0')
			case 'a' <= c && c <= 'f':
				val = val*16 + (c - 'a' + 10)
			case 'A' <= c && c <= 'F':
				val = val*16 + (c - 'A' + 10)
			}
		}
		result[i/2] = val
	}
	return result, nil
}

// GetScannerStatus 获取扫描器状态
// GET /api/v1/scanner/status
func (h *Handler) GetScannerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.error(w, 405, "method not allowed")
		return
	}

	if h.scanner == nil {
		h.error(w, 500, "scanner not available")
		return
	}

	status := h.scanner.GetStatus()
	h.success(w, status)
}

// Health 健康检查
// GET /api/v1/health
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	h.success(w, map[string]string{
		"status": "ok",
	})
}
