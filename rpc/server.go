package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"evm-scanner-go/cmd/config"

	"go.uber.org/zap"
)

// Server JSON-RPC 服务器
type Server struct {
	httpServer *http.Server
	registry   *MethodRegistry
	ethService *EthService
	log        *zap.Logger
	cfg        *config.RPCConfig
}

// NewServer 创建 RPC 服务器
func NewServer(cfg *config.RPCConfig, chainCfg *config.ChainConfig, log *zap.Logger) (*Server, error) {
	// 创建以太坊服务
	ethService, err := NewEthService(chainCfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create eth service: %w", err)
	}

	// 创建方法注册表
	registry := NewMethodRegistry()

	// 注册以太坊方法
	ethService.RegisterMethods(registry)

	// 创建服务器
	server := &Server{
		registry:   registry,
		ethService: ethService,
		log:        log,
		cfg:        cfg,
	}

	// 创建 HTTP 服务器
	mux := http.NewServeMux()
	mux.HandleFunc("/", server.handleRPC)

	// 添加 CORS 支持
	handler := server.corsMiddleware(mux)

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	server.httpServer = &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  time.Duration(cfg.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.WriteTimeout) * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return server, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	s.log.Info("Starting JSON-RPC server",
		zap.String("addr", s.httpServer.Addr),
		zap.Strings("methods", s.registry.Methods()))

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("RPC server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	s.log.Info("Stopping JSON-RPC server")

	// 关闭以太坊服务
	if s.ethService != nil {
		s.ethService.Close()
	}

	return s.httpServer.Shutdown(ctx)
}

// handleRPC 处理 RPC 请求
func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	// 只接受 POST 请求
	if r.Method != http.MethodPost {
		s.writeError(w, nil, ErrInvalidRequest)
		return
	}

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeError(w, nil, ErrParseError)
		return
	}
	defer r.Body.Close()

	// 解析请求
	req, batch, parseErr := ParseRequest(body)
	if parseErr != nil {
		s.writeError(w, nil, parseErr)
		return
	}

	// 处理批量请求
	if batch != nil {
		responses := make(BatchResponse, 0, len(*batch))
		for _, request := range *batch {
			resp := s.processRequest(request)
			if resp != nil {
				responses = append(responses, resp)
			}
		}
		s.writeJSON(w, responses)
		return
	}

	// 处理单个请求
	resp := s.processRequest(req)
	if resp != nil {
		s.writeJSON(w, resp)
	}
}

// processRequest 处理单个请求
func (s *Server) processRequest(req *Request) *Response {
	start := time.Now()

	// 验证请求
	if req.Method == "" {
		return NewErrorResponse(req.ID, ErrInvalidRequest)
	}

	// 执行方法
	result, err := s.registry.Execute(req.Method, req.Params)

	duration := time.Since(start)

	// 记录日志
	if err != nil {
		s.log.Debug("RPC call failed",
			zap.String("method", req.Method),
			zap.Duration("duration", duration),
			zap.Int("error_code", err.Code),
			zap.String("error_msg", err.Message))
		return NewErrorResponse(req.ID, err)
	}

	s.log.Debug("RPC call succeeded",
		zap.String("method", req.Method),
		zap.Duration("duration", duration))

	// 如果是通知（没有 ID），不返回响应
	if req.ID == nil {
		return nil
	}

	return NewResponse(req.ID, result)
}

// writeJSON 写入 JSON 响应
func (s *Server) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.log.Error("Failed to encode response", zap.Error(err))
	}
}

// writeError 写入错误响应
func (s *Server) writeError(w http.ResponseWriter, id interface{}, err *Error) {
	s.writeJSON(w, NewErrorResponse(id, err))
}

// corsMiddleware CORS 中间件
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 设置 CORS 头
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// 处理预检请求
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// RegisterMethod 注册自定义方法
func (s *Server) RegisterMethod(name string, handler MethodHandler) {
	s.registry.Register(name, handler)
}

// GetMethods 获取所有注册的方法
func (s *Server) GetMethods() []string {
	return s.registry.Methods()
}
