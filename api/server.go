package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"evm-scanner-go/cmd/config"
	"evm-scanner-go/cmd/services"

	"go.uber.org/zap"
)

// Server HTTP API 服务器
type Server struct {
	server  *http.Server
	handler *Handler
	log     *zap.Logger
}

// NewServer 创建 API 服务器
func NewServer(cfg *config.APIConfig, rpc *services.RPCService, scanner *services.Scanner, log *zap.Logger) *Server {
	handler := NewHandler(rpc, scanner, log)

	mux := http.NewServeMux()

	// 注册路由
	// 交易相关
	mux.HandleFunc("/api/v1/transaction/send", handler.SendRawTransaction)
	mux.HandleFunc("/api/v1/transaction/receipt/", handler.GetTransactionReceipt)
	mux.HandleFunc("/api/v1/transaction/detail/", handler.GetTransactionDetail)
	mux.HandleFunc("/api/v1/transaction/status/", handler.GetTransactionStatus)

	// 链信息
	mux.HandleFunc("/api/v1/chain/block-number", handler.GetBlockNumber)
	mux.HandleFunc("/api/v1/chain/gas-price", handler.GetGasPrice)
	mux.HandleFunc("/api/v1/chain/estimate-gas", handler.EstimateGas)

	// 账户相关
	mux.HandleFunc("/api/v1/account/balance/", handler.GetBalance)
	mux.HandleFunc("/api/v1/account/nonce/", handler.GetNonce)

	// 扫描器状态
	mux.HandleFunc("/api/v1/scanner/status", handler.GetScannerStatus)

	// 健康检查
	mux.HandleFunc("/api/v1/health", handler.Health)

	// 添加日志中间件
	loggedMux := loggingMiddleware(mux, log)

	// 添加 CORS 中间件（如果需要）
	corsHandler := corsMiddleware(loggedMux)

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	server := &http.Server{
		Addr:         addr,
		Handler:      corsHandler,
		ReadTimeout:  time.Duration(cfg.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.WriteTimeout) * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return &Server{
		server:  server,
		handler: handler,
		log:     log,
	}
}

// Start 启动服务器（非阻塞）
func (s *Server) Start() error {
	s.log.Info("Starting API server", zap.String("addr", s.server.Addr))

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("API server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	s.log.Info("Stopping API server")
	return s.server.Shutdown(ctx)
}

// loggingMiddleware 日志中间件
func loggingMiddleware(next http.Handler, log *zap.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 包装 ResponseWriter 以获取状态码
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		duration := time.Since(start)

		log.Info("HTTP request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", wrapped.statusCode),
			zap.Duration("duration", duration),
			zap.String("remote_addr", r.RemoteAddr),
		)
	})
}

// responseWriter 包装器，用于获取状态码
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// corsMiddleware CORS 中间件
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 设置 CORS 头
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// 处理预检请求
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// RegisterRoutes 用于自定义路由注册（可选）
func (s *Server) RegisterRoutes(pattern string, handler http.HandlerFunc) {
	// 此方法预留用于扩展
}

// pathMatch 路径匹配辅助函数
func pathMatch(pattern, path string) bool {
	if strings.HasSuffix(pattern, "/") {
		return strings.HasPrefix(path, pattern)
	}
	return path == pattern
}
