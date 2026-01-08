package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"evm-scanner-go/cmd/config"
	"evm-scanner-go/cmd/internal"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"go.uber.org/zap"
)

// MQProducer RocketMQ生产者
type MQProducer struct {
	producer  rocketmq.Producer
	topic     string
	timeout   time.Duration
	chainName string
	chainID   int64
	log       *zap.Logger
}

// NewMQProducer 创建RocketMQ生产者
func NewMQProducer(cfg *config.RocketMQConfig, chainName string, chainID int64, log *zap.Logger) (*MQProducer, error) {
	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{cfg.NameServer}),
		producer.WithGroupName(cfg.GroupName),
		producer.WithRetry(cfg.RetryTimes),
		producer.WithSendMsgTimeout(cfg.TimeoutDuration()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	if err := p.Start(); err != nil {
		return nil, fmt.Errorf("failed to start producer: %w", err)
	}

	log.Info("RocketMQ producer started",
		zap.String("name_server", cfg.NameServer),
		zap.String("group", cfg.GroupName),
		zap.String("topic", cfg.Topic))

	return &MQProducer{
		producer:  p,
		topic:     cfg.Topic,
		timeout:   cfg.TimeoutDuration(),
		chainName: chainName,
		chainID:   chainID,
		log:       log,
	}, nil
}

// SendTransaction 发送交易消息
func (p *MQProducer) SendTransaction(ctx context.Context, tx *internal.Transaction) error {
	msg := internal.MQMessage{
		Type:      "transaction",
		ChainName: p.chainName,
		ChainID:   p.chainID,
		Data:      tx,
		Timestamp: time.Now().UnixMilli(),
	}

	return p.sendMessage(ctx, msg, tx.TxHash)
}

// SendTokenTransfer 发送Token转账消息
func (p *MQProducer) SendTokenTransfer(ctx context.Context, transfer *internal.TokenTransfer) error {
	msg := internal.MQMessage{
		Type:      "token_transfer",
		ChainName: p.chainName,
		ChainID:   p.chainID,
		Data:      transfer,
		Timestamp: time.Now().UnixMilli(),
	}

	key := fmt.Sprintf("%s_%d", transfer.TxHash, transfer.LogIndex)
	return p.sendMessage(ctx, msg, key)
}

// SendInternalTransfer 发送合约内部转账消息（原生代币）
func (p *MQProducer) SendInternalTransfer(ctx context.Context, transfer *internal.InternalTransfer) error {
	msg := internal.MQMessage{
		Type:      "internal_transfer",
		ChainName: p.chainName,
		ChainID:   p.chainID,
		Data:      transfer,
		Timestamp: time.Now().UnixMilli(),
	}

	key := fmt.Sprintf("%s_internal_%d", transfer.TxHash, transfer.TraceIndex)
	return p.sendMessage(ctx, msg, key)
}

// SendBatchTransactions 批量发送交易消息
func (p *MQProducer) SendBatchTransactions(ctx context.Context, txs []*internal.Transaction) error {
	for _, tx := range txs {
		if err := p.SendTransaction(ctx, tx); err != nil {
			p.log.Error("Failed to send transaction",
				zap.String("tx_hash", tx.TxHash),
				zap.Error(err))
			// 继续发送其他交易
		}
	}
	return nil
}

// sendMessage 发送消息到RocketMQ
func (p *MQProducer) sendMessage(ctx context.Context, msg internal.MQMessage, key string) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	mqMsg := &primitive.Message{
		Topic: p.topic,
		Body:  body,
	}
	mqMsg.WithKeys([]string{key})

	// 根据交易类型设置Tag
	switch data := msg.Data.(type) {
	case *internal.Transaction:
		mqMsg.WithTag(data.TxType.String())
	case *internal.TokenTransfer:
		mqMsg.WithTag("token_" + data.TxType.String())
	case *internal.InternalTransfer:
		mqMsg.WithTag("internal_" + data.TxType.String())
	}

	result, err := p.producer.SendSync(ctx, mqMsg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	p.log.Debug("Message sent to RocketMQ",
		zap.String("key", key),
		zap.String("topic", p.topic),
		zap.String("msg_id", result.MsgID))

	return nil
}

// Close 关闭生产者
func (p *MQProducer) Close() error {
	if p.producer != nil {
		return p.producer.Shutdown()
	}
	return nil
}

// SendBlockNotification 发送区块处理完成通知
func (p *MQProducer) SendBlockNotification(ctx context.Context, blockInfo *internal.BlockInfo) error {
	msg := internal.MQMessage{
		Type:      "block_scanned",
		ChainName: p.chainName,
		ChainID:   p.chainID,
		Data:      blockInfo,
		Timestamp: time.Now().UnixMilli(),
	}

	key := fmt.Sprintf("block_%d", blockInfo.BlockNumber)
	return p.sendMessage(ctx, msg, key)
}
