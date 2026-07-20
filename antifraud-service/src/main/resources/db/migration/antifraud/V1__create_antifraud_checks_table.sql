CREATE TABLE antifraud_checks (
    id          UUID PRIMARY KEY,
    transfer_id UUID NOT NULL,
    resultado   VARCHAR(20) NOT NULL,
    motivo      VARCHAR(50),
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_antifraud_checks_transfer_id ON antifraud_checks (transfer_id);
