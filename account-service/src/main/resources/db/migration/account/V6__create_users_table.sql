CREATE TABLE users (
    id             UUID PRIMARY KEY,
    email          VARCHAR(255) NOT NULL,
    password_hash  VARCHAR(255) NOT NULL,
    account_id     UUID NOT NULL,
    CONSTRAINT uq_users_email UNIQUE (email)
);
