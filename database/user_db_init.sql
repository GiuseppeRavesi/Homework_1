CREATE TABLE IF NOT EXISTS users (
    email TEXT PRIMARY KEY,
    fiscal_code TEXT,
    bank_account TEXT
);
CREATE TABLE IF NOT EXISTS processed_requests (
    request_id VARCHAR(255) PRIMARY KEY
);