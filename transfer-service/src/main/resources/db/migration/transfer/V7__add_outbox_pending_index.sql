-- Sustenta a seleção do poller do Outbox (reivindicarPendentes): o NOT EXISTS procura, por
-- agregado, eventos PENDING anteriores. Índice parcial: só as linhas PENDING (poucas) entram,
-- então continua pequeno mesmo com milhões de linhas PUBLISHED na tabela.
CREATE INDEX IF NOT EXISTS idx_outbox_events_pending_aggregate
    ON outbox_events (aggregate_id, created_at) WHERE status = 'PENDING';