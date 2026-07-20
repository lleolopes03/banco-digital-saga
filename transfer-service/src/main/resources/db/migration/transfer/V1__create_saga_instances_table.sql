CREATE TABLE saga_instances (
    id                  UUID PRIMARY KEY,
    account_origem_id   UUID NOT NULL,
    account_destino_id  UUID NOT NULL,
    valor               NUMERIC(19,2) NOT NULL,
    status              VARCHAR(30) NOT NULL,
    current_step        VARCHAR(50),
    failure_reason      VARCHAR(255),
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_saga_instances_status ON saga_instances (status);
