-- Contas sinteticas para o teste de carga (load-tests/ramp-test.mjs).
--
-- Por que tantas contas: o sistema tem limites de negocio POR CONTA DE ORIGEM — rate limit de
-- 10 req/min no transfer-service e a regra de velocidade do antifraude (mais de 5 transferencias
-- em 60s vira VELOCITY_EXCEDIDA). Para medir CAPACIDADE sem esbarrar nessas regras, a carga e
-- distribuida em rodizio entre muitas origens: com 200 contas e 12 tx/s, cada uma envia ~3,6/min.
--
-- IDs deterministicos (00000000-0000-4000-8000-<n em hex, 12 digitos>) para o script de carga
-- conseguir gera-los sem consultar o banco, e para o filtro de analise isolar so a carga.
-- Saldo alto (1.000.000,00) para que saldo nunca seja o fator limitante.
-- Rodar contra o account_db:
--   docker exec -i bank-postgres-account psql -U account -d account_db < load-tests/seed-contas-carga.sql

INSERT INTO accounts (id, owner_name, owner_document, balance, status, version, created_at, reserved_balance)
SELECT ('00000000-0000-4000-8000-' || lpad(to_hex(i), 12, '0'))::uuid,
       'Conta de carga ' || i,
       '9' || lpad(i::text, 10, '0'),
       1000000.00,
       'ACTIVE',
       0,
       now(),
       0
FROM generate_series(1, 200) AS i
ON CONFLICT DO NOTHING;

SELECT count(*) AS contas_de_carga FROM accounts WHERE id::text LIKE '00000000-0000-4000-8000-%';
