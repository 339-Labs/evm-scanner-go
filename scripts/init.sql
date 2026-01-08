-- EVM Chain Scanner 数据库初始化脚本

CREATE DATABASE IF NOT EXISTS evm_scanner DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE evm_scanner;

-- 链上交易记录表
CREATE TABLE IF NOT EXISTS chain_transactions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tx_hash VARCHAR(66) NOT NULL COMMENT '交易哈希',
    block_number BIGINT UNSIGNED NOT NULL COMMENT '区块号',
    block_hash VARCHAR(66) NOT NULL COMMENT '区块哈希',
    block_time DATETIME NOT NULL COMMENT '区块时间',
    from_address VARCHAR(42) NOT NULL COMMENT '发送地址',
    to_address VARCHAR(42) COMMENT '接收地址',
    value VARCHAR(78) NOT NULL COMMENT '交易金额(wei)',
    gas_price VARCHAR(78) COMMENT 'Gas价格',
    gas_used BIGINT UNSIGNED NOT NULL COMMENT 'Gas消耗',
    nonce BIGINT UNSIGNED NOT NULL COMMENT 'Nonce',
    tx_index INT UNSIGNED NOT NULL COMMENT '交易索引',
    input TEXT COMMENT '输入数据',
    status TINYINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '状态: 1=成功, 0=失败',
    contract_address VARCHAR(42) COMMENT '合约地址',
    tx_type TINYINT NOT NULL COMMENT '交易类型: 0=未知, 1=充值, 2=提现, 3=内部转账',
    chain_id BIGINT NOT NULL COMMENT '链ID',
    chain_name VARCHAR(32) NOT NULL COMMENT '链名称',
    is_from_internal TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'from是否为内部地址',
    is_to_internal TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'to是否为内部地址',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_tx_hash (tx_hash),
    INDEX idx_block_number (block_number),
    INDEX idx_block_time (block_time),
    INDEX idx_from_address (from_address),
    INDEX idx_to_address (to_address),
    INDEX idx_tx_type (tx_type),
    INDEX idx_chain_id (chain_id),
    INDEX idx_chain_name (chain_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='链上交易记录表';

-- Token转账记录表
CREATE TABLE IF NOT EXISTS token_transfers (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tx_hash VARCHAR(66) NOT NULL COMMENT '交易哈希',
    log_index INT UNSIGNED NOT NULL COMMENT '日志索引',
    block_number BIGINT UNSIGNED NOT NULL COMMENT '区块号',
    contract_address VARCHAR(42) NOT NULL COMMENT '合约地址',
    from_address VARCHAR(42) NOT NULL COMMENT '发送地址',
    to_address VARCHAR(42) NOT NULL COMMENT '接收地址',
    value VARCHAR(78) NOT NULL COMMENT '转账金额',
    token_symbol VARCHAR(32) COMMENT 'Token符号',
    token_decimals TINYINT UNSIGNED NOT NULL DEFAULT 18 COMMENT 'Token精度',
    tx_type TINYINT NOT NULL COMMENT '交易类型: 0=未知, 1=充值, 2=提现, 3=内部转账',
    chain_id BIGINT NOT NULL COMMENT '链ID',
    chain_name VARCHAR(32) NOT NULL COMMENT '链名称',
    is_from_internal TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'from是否为内部地址',
    is_to_internal TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'to是否为内部地址',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    UNIQUE KEY uk_tx_log (tx_hash, log_index),
    INDEX idx_block_number (block_number),
    INDEX idx_contract_address (contract_address),
    INDEX idx_from_address (from_address),
    INDEX idx_to_address (to_address),
    INDEX idx_tx_type (tx_type),
    INDEX idx_chain_id (chain_id),
    INDEX idx_chain_name (chain_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Token转账记录表';

-- 扫描进度表
CREATE TABLE IF NOT EXISTS scan_progress (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    chain_name VARCHAR(32) NOT NULL COMMENT '链名称',
    chain_id BIGINT NOT NULL COMMENT '链ID',
    last_block BIGINT UNSIGNED NOT NULL COMMENT '最后扫描的区块',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    UNIQUE KEY uk_chain_name (chain_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='扫描进度表';

