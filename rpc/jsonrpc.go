package rpc

import (
	"encoding/json"
	"fmt"
)

// JSON-RPC 2.0 协议定义

// Request JSON-RPC 请求结构
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      interface{}     `json:"id,omitempty"`
}

// Response JSON-RPC 响应结构
type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *Error      `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

// Error JSON-RPC 错误结构
type Error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// 标准 JSON-RPC 错误码
const (
	ParseError          = -32700 // 解析错误
	InvalidRequest      = -32600 // 无效请求
	MethodNotFound      = -32601 // 方法不存在
	InvalidParams       = -32602 // 无效参数
	InternalError       = -32603 // 内部错误
	ServerError         = -32000 // 服务端错误（范围 -32000 到 -32099）
	TransactionRejected = -32003 // 交易被拒绝
)

// NewError 创建错误
func NewError(code int, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

// NewErrorWithData 创建带数据的错误
func NewErrorWithData(code int, message string, data interface{}) *Error {
	return &Error{
		Code:    code,
		Message: message,
		Data:    data,
	}
}

// Error 实现 error 接口
func (e *Error) Error() string {
	return fmt.Sprintf("RPC error %d: %s", e.Code, e.Message)
}

// 预定义错误
var (
	ErrParseError     = NewError(ParseError, "Parse error")
	ErrInvalidRequest = NewError(InvalidRequest, "Invalid request")
	ErrMethodNotFound = NewError(MethodNotFound, "Method not found")
	ErrInvalidParams  = NewError(InvalidParams, "Invalid params")
	ErrInternalError  = NewError(InternalError, "Internal error")
)

// NewResponse 创建成功响应
func NewResponse(id interface{}, result interface{}) *Response {
	return &Response{
		JSONRPC: "2.0",
		Result:  result,
		ID:      id,
	}
}

// NewErrorResponse 创建错误响应
func NewErrorResponse(id interface{}, err *Error) *Response {
	return &Response{
		JSONRPC: "2.0",
		Error:   err,
		ID:      id,
	}
}

// BatchRequest 批量请求
type BatchRequest []*Request

// BatchResponse 批量响应
type BatchResponse []*Response

// ParseRequest 解析请求
func ParseRequest(data []byte) (*Request, *BatchRequest, *Error) {
	// 尝试解析为单个请求
	var req Request
	if err := json.Unmarshal(data, &req); err == nil {
		if req.JSONRPC != "2.0" {
			return nil, nil, ErrInvalidRequest
		}
		return &req, nil, nil
	}

	// 尝试解析为批量请求
	var batch BatchRequest
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, nil, ErrParseError
	}

	if len(batch) == 0 {
		return nil, nil, ErrInvalidRequest
	}

	return nil, &batch, nil
}

// MethodHandler RPC 方法处理函数类型
type MethodHandler func(params json.RawMessage) (interface{}, *Error)

// MethodRegistry 方法注册表
type MethodRegistry struct {
	methods map[string]MethodHandler
}

// NewMethodRegistry 创建方法注册表
func NewMethodRegistry() *MethodRegistry {
	return &MethodRegistry{
		methods: make(map[string]MethodHandler),
	}
}

// Register 注册方法
func (r *MethodRegistry) Register(name string, handler MethodHandler) {
	r.methods[name] = handler
}

// Get 获取方法处理函数
func (r *MethodRegistry) Get(name string) (MethodHandler, bool) {
	handler, ok := r.methods[name]
	return handler, ok
}

// Execute 执行方法
func (r *MethodRegistry) Execute(method string, params json.RawMessage) (interface{}, *Error) {
	handler, ok := r.Get(method)
	if !ok {
		return nil, ErrMethodNotFound
	}
	return handler(params)
}

// HasMethod 检查方法是否存在
func (r *MethodRegistry) HasMethod(name string) bool {
	_, ok := r.methods[name]
	return ok
}

// Methods 获取所有注册的方法名
func (r *MethodRegistry) Methods() []string {
	methods := make([]string, 0, len(r.methods))
	for name := range r.methods {
		methods = append(methods, name)
	}
	return methods
}
