package com.bancodigital.notification.domain;

/**
 * SENT é o único status possível nesta fatia — não existe integração real de envio ainda (ver
 * README), então toda notificação criada já nasce SENT. PENDING/FAILED ficam reservados para
 * quando um canal de envio de verdade (Anthropic API / Z-API) existir.
 */
public enum NotificationStatus {
    PENDING,
    SENT,
    FAILED
}
