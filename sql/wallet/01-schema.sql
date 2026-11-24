CREATE TABLE clients (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at DATETIME NOT NULL
);

CREATE TABLE accounts (
    id VARCHAR(36) PRIMARY KEY,
    client_id VARCHAR(36) NOT NULL,
    balance DECIMAL(20, 8) NOT NULL,
    created_at DATETIME NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id)
);

CREATE TABLE transactions (
    id VARCHAR(36) PRIMARY KEY,
    account_id_from VARCHAR(36) NOT NULL,
    account_id_to VARCHAR(36) NOT NULL,
    amount DECIMAL(20, 8) NOT NULL,
    created_at DATETIME NOT NULL,
    FOREIGN KEY (account_id_from) REFERENCES accounts(id),
    FOREIGN KEY (account_id_to) REFERENCES accounts(id)
);
