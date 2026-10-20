package com.bancodigital.account.application;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

/**
 * Política única de retry para conflito de lock otimista (@Version em Account): 3 tentativas com
 * espera de 20 ms dobrando a cada uma. Um conflito é raro e se resolve relendo a conta; o custo da
 * nova tentativa só é pago quando o conflito realmente acontece. Quem usa declara um método
 * {@code @Recover} para o que fazer quando as tentativas se esgotam.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Retryable(retryFor = OptimisticLockingFailureException.class, maxAttempts = 3,
        backoff = @Backoff(delay = 20, multiplier = 2))
public @interface RetryOnOptimisticLock {
}
