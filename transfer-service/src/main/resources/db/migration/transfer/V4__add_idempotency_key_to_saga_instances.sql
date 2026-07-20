ALTER TABLE saga_instances ADD COLUMN idempotency_key VARCHAR(255);

-- Índice único parcial: só se aplica a linhas com idempotency_key preenchido, já que a maior
-- parte das transferências não usa o header Idempotency-Key e não pode colidir entre si por
-- terem o valor NULL em comum (Postgres já trata múltiplos NULLs como não-iguais em índices
-- únicos comuns, mas o WHERE aqui deixa a intenção explícita e evita indexar as linhas sem chave).
CREATE UNIQUE INDEX idx_saga_instances_idempotency_key ON saga_instances (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
