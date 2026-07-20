package com.bancodigital.transfer.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Máquina de estados pura, sem dependência de nenhum framework: recebe uma {@link SagaInstance}
 * e um {@link SagaEvent}, valida se a transição é permitida a partir do estado atual da saga e,
 * se for, aplica a mudança. Se não for, nada na saga é alterado e uma
 * {@link InvalidSagaTransitionException} é lançada.
 */
public class SagaStateMachine {

    private static final Map<SagaStatus, Set<SagaStatus>> TRANSICOES_VALIDAS = new EnumMap<>(SagaStatus.class);

    static {
        // FAILED é alcançável a partir de STARTED, ANTIFRAUD_PENDING e ANTIFRAUD_APPROVED (além
        // de RESERVED/ANTIFRAUD_REJECTED/DEBITED/CREDITED, já previstos) porque o
        // SagaTimeoutScheduler pode disparar compensação/falha a partir de qualquer estado
        // intermediário não-terminal em que a saga fique travada, não só a partir dos estados
        // que o fluxo feliz normalmente atravessa antes de uma rejeição explícita.
        TRANSICOES_VALIDAS.put(SagaStatus.STARTED, EnumSet.of(SagaStatus.RESERVED, SagaStatus.FAILED));
        TRANSICOES_VALIDAS.put(SagaStatus.RESERVED, EnumSet.of(SagaStatus.ANTIFRAUD_PENDING, SagaStatus.FAILED));
        TRANSICOES_VALIDAS.put(SagaStatus.ANTIFRAUD_PENDING,
                EnumSet.of(SagaStatus.ANTIFRAUD_APPROVED, SagaStatus.ANTIFRAUD_REJECTED, SagaStatus.FAILED));
        TRANSICOES_VALIDAS.put(SagaStatus.ANTIFRAUD_APPROVED, EnumSet.of(SagaStatus.DEBITED, SagaStatus.FAILED));
        TRANSICOES_VALIDAS.put(SagaStatus.ANTIFRAUD_REJECTED, EnumSet.of(SagaStatus.FAILED));
        TRANSICOES_VALIDAS.put(SagaStatus.DEBITED, EnumSet.of(SagaStatus.CREDITED, SagaStatus.FAILED));
        TRANSICOES_VALIDAS.put(SagaStatus.CREDITED, EnumSet.of(SagaStatus.COMPLETED, SagaStatus.FAILED));
        TRANSICOES_VALIDAS.put(SagaStatus.COMPLETED, EnumSet.noneOf(SagaStatus.class));
        TRANSICOES_VALIDAS.put(SagaStatus.FAILED, EnumSet.noneOf(SagaStatus.class));
    }

    public void transicionar(SagaInstance saga, SagaEvent evento) {
        SagaStatus estadoAtual = saga.getStatus();
        SagaStatus proximoEstado = evento.getProximoEstado();

        Set<SagaStatus> estadosPermitidos = TRANSICOES_VALIDAS.getOrDefault(estadoAtual, Set.of());
        if (!estadosPermitidos.contains(proximoEstado)) {
            throw new InvalidSagaTransitionException(estadoAtual, evento, proximoEstado);
        }

        saga.aplicarTransicao(proximoEstado, evento.name());
    }
}
