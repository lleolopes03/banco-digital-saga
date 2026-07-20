package com.bancodigital.account.domain;

public enum OutboxEventStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
