CREATE TABLE accounts (
    id              UUID PRIMARY KEY,
    owner_name      VARCHAR(255) NOT NULL,
    owner_document  VARCHAR(20)  NOT NULL,
    balance         NUMERIC(19,2) NOT NULL DEFAULT 0,
    status          VARCHAR(20)  NOT NULL,
    version         BIGINT       NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uq_accounts_owner_document UNIQUE (owner_document),
    CONSTRAINT ck_accounts_balance_non_negative CHECK (balance >= 0)
);
