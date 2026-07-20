CREATE TABLE blacklisted_accounts (
    account_id UUID PRIMARY KEY,
    reason     VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- Seed fictício para uso em testes e ambiente de dev
INSERT INTO blacklisted_accounts (account_id, reason, created_at) VALUES
    ('11111111-1111-1111-1111-111111111111', 'Conta fictícia bloqueada para testes - fraude confirmada', now()),
    ('22222222-2222-2222-2222-222222222222', 'Conta fictícia bloqueada para testes - suspeita de lavagem de dinheiro', now()),
    ('33333333-3333-3333-3333-333333333333', 'Conta fictícia bloqueada para testes - múltiplas reclamações de golpe', now());
