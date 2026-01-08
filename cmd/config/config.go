package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config 全局配置结构
type Config struct {
	Chain    ChainConfig    `mapstructure:"chain"`
	MySQL    MySQLConfig    `mapstructure:"mysql"`
	Redis    RedisConfig    `mapstructure:"redis"`
	RocketMQ RocketMQConfig `mapstructure:"rocketmq"`
	Log      LogConfig      `mapstructure:"log"`
	API      APIConfig      `mapstructure:"api"`
	RPC      RPCConfig      `mapstructure:"rpc"`
}

// ChainConfig 链配置
type ChainConfig struct {
	Name          string `mapstructure:"name"`
	ChainID       int64  `mapstructure:"chain_id"`
	RPCURL        string `mapstructure:"rpc_url"`
	WSURL         string `mapstructure:"ws_url"`
	StartBlock    uint64 `mapstructure:"start_block"`
	Confirmations uint64 `mapstructure:"confirmations"`
	BatchSize     int    `mapstructure:"batch_size"`
	PollInterval  int    `mapstructure:"poll_interval"`

	// 并发配置
	RPCPoolSize     int `mapstructure:"rpc_pool_size"`     // RPC 连接池大小
	BlockWorkers    int `mapstructure:"block_workers"`     // 区块处理并发数
	TxWorkers       int `mapstructure:"tx_workers"`        // 交易处理并发数
	BlockQueueSize  int `mapstructure:"block_queue_size"`  // 区块队列大小
	MaxRetries      int `mapstructure:"max_retries"`       // 最大重试次数
	RetryIntervalMs int `mapstructure:"retry_interval_ms"` // 重试间隔（毫秒）

	// 内部转账追踪配置
	TraceEnabled bool   `mapstructure:"trace_enabled"` // 是否启用 trace（解析合约内部转账）
	TraceType    string `mapstructure:"trace_type"`    // trace 类型: "debug" (Geth) 或 "trace" (Parity/Erigon)
}

// MySQLConfig MySQL配置
type MySQLConfig struct {
	Host            string `mapstructure:"host"`
	Port            int    `mapstructure:"port"`
	User            string `mapstructure:"user"`
	Password        string `mapstructure:"password"`
	Database        string `mapstructure:"database"`
	MaxOpenConns    int    `mapstructure:"max_open_conns"`
	MaxIdleConns    int    `mapstructure:"max_idle_conns"`
	ConnMaxLifetime int    `mapstructure:"conn_max_lifetime"`
}

// DSN 返回MySQL连接字符串
func (c *MySQLConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		c.User, c.Password, c.Host, c.Port, c.Database)
}

// RedisConfig Redis配置
type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	BloomKey string `mapstructure:"bloom_key"`
}

// Addr 返回Redis地址
func (c *RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// RocketMQConfig RocketMQ配置
type RocketMQConfig struct {
	NameServer string `mapstructure:"name_server"`
	GroupName  string `mapstructure:"group_name"`
	Topic      string `mapstructure:"topic"`
	RetryTimes int    `mapstructure:"retry_times"`
	Timeout    int    `mapstructure:"timeout"`
}

// TimeoutDuration 返回超时时间
func (c *RocketMQConfig) TimeoutDuration() time.Duration {
	return time.Duration(c.Timeout) * time.Millisecond
}

// LogConfig 日志配置
type LogConfig struct {
	Level      string `mapstructure:"level"`
	File       string `mapstructure:"file"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`
}

// APIConfig API 服务配置
type APIConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	ReadTimeout  int    `mapstructure:"read_timeout"`  // 读取超时（秒）
	WriteTimeout int    `mapstructure:"write_timeout"` // 写入超时（秒）
}

// RPCConfig JSON-RPC 服务配置
type RPCConfig struct {
	Enabled      bool   `mapstructure:"enabled"`       // 是否启用
	Host         string `mapstructure:"host"`          // 监听地址
	Port         int    `mapstructure:"port"`          // 监听端口
	ReadTimeout  int    `mapstructure:"read_timeout"`  // 读取超时（秒）
	WriteTimeout int    `mapstructure:"write_timeout"` // 写入超时（秒）
}

var GlobalConfig *Config

// Load 加载配置文件
func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := &Config{}
	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	GlobalConfig = config
	return config, nil
}

// LoadFromDefault 从默认路径加载配置
func LoadFromDefault() (*Config, error) {
	return Load("configs/config.yaml")
}
