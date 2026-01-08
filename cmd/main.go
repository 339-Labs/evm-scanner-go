package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"evm-scanner-go/api"
	"evm-scanner-go/cmd/config"
	"evm-scanner-go/cmd/dao"
	"evm-scanner-go/cmd/internal"
	"evm-scanner-go/cmd/services"
	"evm-scanner-go/rpc"

	"go.uber.org/zap"
)

var (
	configPath = flag.String("config", "configs/config.yaml", "配置文件路径")
	version    = "1.0.0"
)

func main() {
	flag.Parse()

	fmt.Printf("EVM Chain Scanner v%s\n", version)
	fmt.Printf("Loading config from: %s\n", *configPath)

	// 加载配置
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 初始化日志
	log, err := internal.InitLogger(&cfg.Log)
	if err != nil {
		fmt.Printf("Failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	log.Info("Starting EVM Chain Scanner",
		zap.String("version", version),
		zap.String("chain", cfg.Chain.Name),
		zap.Int64("chain_id", cfg.Chain.ChainID))

	// 初始化MySQL
	if err := dao.InitMySQL(&cfg.MySQL, log); err != nil {
		log.Fatal("Failed to init MySQL", zap.Error(err))
	}
	defer dao.CloseMySQL()

	// 初始化Redis
	if err := dao.InitRedis(&cfg.Redis, log); err != nil {
		log.Fatal("Failed to init Redis", zap.Error(err))
	}
	defer dao.CloseRedis()

	// 创建Bloom过滤器
	bloomFilter := dao.NewAddressBloomFilter(cfg.Redis.BloomKey, log)

	// 初始化Bloom过滤器（如果需要）
	ctx := context.Background()
	if err := bloomFilter.CreateBloomFilter(ctx, 0.001, 10000000); err != nil {
		log.Warn("Failed to create bloom filter", zap.Error(err))
	}

	// 创建DAO
	txDAO := dao.NewTransactionDAO()

	// 创建RocketMQ生产者
	mqProducer, err := services.NewMQProducer(&cfg.RocketMQ, cfg.Chain.Name, cfg.Chain.ChainID, log)
	if err != nil {
		log.Fatal("Failed to create MQ producer", zap.Error(err))
	}
	defer mqProducer.Close()

	// 创建扫描器
	scanner, err := services.NewScanner(&cfg.Chain, bloomFilter, txDAO, mqProducer, log)
	if err != nil {
		log.Fatal("Failed to create scanner", zap.Error(err))
	}

	// 启动扫描
	scanCtx, cancel := context.WithCancel(context.Background())
	if err := scanner.Start(scanCtx); err != nil {
		log.Fatal("Failed to start scanner", zap.Error(err))
	}

	// 创建 HTTP RPC 服务（用于 RESTful API）
	rpcService, err := services.NewRPCService(&cfg.Chain, log)
	if err != nil {
		log.Fatal("Failed to create RPC service", zap.Error(err))
	}
	defer rpcService.Close()

	// 创建并启动 RESTful API 服务器
	apiServer := api.NewServer(&cfg.API, rpcService, scanner, log)
	if err := apiServer.Start(); err != nil {
		log.Fatal("Failed to start API server", zap.Error(err))
	}

	// 创建并启动 JSON-RPC 服务器（以太坊标准 RPC 接口）
	var rpcServer *rpc.Server
	if cfg.RPC.Enabled {
		rpcServer, err = rpc.NewServer(&cfg.RPC, &cfg.Chain, log)
		if err != nil {
			log.Fatal("Failed to create JSON-RPC server", zap.Error(err))
		}
		if err := rpcServer.Start(); err != nil {
			log.Fatal("Failed to start JSON-RPC server", zap.Error(err))
		}
	}

	// 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Info("All services are running. Press Ctrl+C to stop.",
		zap.Int("api_port", cfg.API.Port),
		zap.Int("rpc_port", cfg.RPC.Port),
		zap.Bool("rpc_enabled", cfg.RPC.Enabled))

	sig := <-sigCh
	log.Info("Received signal, shutting down...", zap.String("signal", sig.String()))

	// 优雅停止
	cancel()
	scanner.Stop()

	// 停止服务器
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := apiServer.Stop(shutdownCtx); err != nil {
		log.Error("Failed to stop API server gracefully", zap.Error(err))
	}

	if rpcServer != nil {
		if err := rpcServer.Stop(shutdownCtx); err != nil {
			log.Error("Failed to stop JSON-RPC server gracefully", zap.Error(err))
		}
	}

	log.Info("All services stopped gracefully")
}
