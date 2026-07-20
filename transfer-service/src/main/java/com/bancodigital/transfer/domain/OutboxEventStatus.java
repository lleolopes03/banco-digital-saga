package com.bancodigital.transfer.domain;

public enum OutboxEventStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
