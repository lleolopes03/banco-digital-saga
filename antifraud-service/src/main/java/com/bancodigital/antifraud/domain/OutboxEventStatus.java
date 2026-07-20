package com.bancodigital.antifraud.domain;

public enum OutboxEventStatus {
    PENDING,
    PUBLISHED,
    FAILED
}
