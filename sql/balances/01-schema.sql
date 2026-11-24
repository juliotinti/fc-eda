CREATE TABLE balances (
    account_id VARCHAR(36) PRIMARY KEY,
    balance DECIMAL(20, 8) NOT NULL,
    kafka_offset BIGINT NOT NULL DEFAULT -1
);
