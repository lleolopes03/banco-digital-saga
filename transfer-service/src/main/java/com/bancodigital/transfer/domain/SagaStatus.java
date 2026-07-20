package com.bancodigital.transfer.domain;

public enum SagaStatus {
    STARTED,
    RESERVED,
    ANTIFRAUD_PENDING,
    ANTIFRAUD_APPROVED,
    ANTIFRAUD_REJECTED,
    DEBITED,
    CREDITED,
    COMPLETED,
    FAILED
}
