CREATE TABLE transaction_log_entries (
    id                      UUID PRIMARY KEY,
    account_id              UUID NOT NULL,
    type                    VARCHAR(20) NOT NULL,
    amount                  NUMERIC(19,2) NOT NULL,
    balance_after           NUMERIC(19,2) NOT NULL,
    related_transaction_id  UUID,
    description             VARCHAR(255),
    created_at              TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_transaction_log_account_created_at
    ON transaction_log_entries (account_id, created_at DESC);
