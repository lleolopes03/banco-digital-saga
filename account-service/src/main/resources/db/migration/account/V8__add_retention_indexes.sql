-- Apoia a limpeza por retencao (RetentionCleanupScheduler): sem estes indices, cada execucao
-- varreria a tabela inteira para achar as linhas antigas.
CREATE INDEX IF NOT EXISTS idx_outbox_events_published_at ON outbox_events (published_at) WHERE status = 'PUBLISHED';
CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at ON processed_events (processed_at);
