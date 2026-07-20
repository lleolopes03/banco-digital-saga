ALTER TABLE accounts
    ADD COLUMN reserved_balance NUMERIC(19,2) NOT NULL DEFAULT 0;

ALTER TABLE accounts
    ADD CONSTRAINT ck_accounts_reserved_balance_non_negative CHECK (reserved_balance >= 0);
