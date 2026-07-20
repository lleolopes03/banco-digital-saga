package com.bancodigital.transfer.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SagaStateMachineTest {

    private final SagaStateMachine stateMachine = new SagaStateMachine();

    private SagaInstance criarSaga() {
        return SagaInstance.iniciar(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), new BigDecimal("100.00"));
    }

    @Test
    void deveTransicionarDeStartedParaReservedComSucesso() {
        SagaInstance saga = criarSaga();

        stateMachine.transicionar(saga, SagaEvent.RESERVAR_SALDO);

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.RESERVED);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaEvent.RESERVAR_SALDO.name());
    }

    @Test
    void deveLancarExcecaoAoTentarTransicaoInvalidaDeStartedParaCompleted() {
        SagaInstance saga = criarSaga();

        assertThatThrownBy(() -> stateMachine.transicionar(saga, SagaEvent.COMPLETAR))
                .isInstanceOf(InvalidSagaTransitionException.class);

        // uma transição rejeitada não deve deixar a saga em estado intermediário/inconsistente
        assertThat(saga.getStatus()).isEqualTo(SagaStatus.STARTED);
        assertThat(saga.getCurrentStep()).isEqualTo(SagaStatus.STARTED.name());
    }

    @ParameterizedTest
    @MethodSource("transicoesValidas")
    void deveAplicarCadaTransicaoValidaDoDiagrama(SagaStatus estadoAtual, SagaEvent evento, SagaStatus estadoEsperado) {
        SagaInstance saga = criarSagaNoEstado(estadoAtual);

        stateMachine.transicionar(saga, evento);

        assertThat(saga.getStatus()).isEqualTo(estadoEsperado);
        assertThat(saga.getCurrentStep()).isEqualTo(evento.name());
    }

    private static Stream<Arguments> transicoesValidas() {
        return Stream.of(
                Arguments.of(SagaStatus.STARTED, SagaEvent.RESERVAR_SALDO, SagaStatus.RESERVED),
                Arguments.of(SagaStatus.RESERVED, SagaEvent.SOLICITAR_ANTIFRAUDE, SagaStatus.ANTIFRAUD_PENDING),
                Arguments.of(SagaStatus.ANTIFRAUD_PENDING, SagaEvent.APROVAR_ANTIFRAUDE, SagaStatus.ANTIFRAUD_APPROVED),
                Arguments.of(SagaStatus.ANTIFRAUD_APPROVED, SagaEvent.DEBITAR, SagaStatus.DEBITED),
                Arguments.of(SagaStatus.DEBITED, SagaEvent.CREDITAR, SagaStatus.CREDITED),
                Arguments.of(SagaStatus.CREDITED, SagaEvent.COMPLETAR, SagaStatus.COMPLETED),
                Arguments.of(SagaStatus.ANTIFRAUD_PENDING, SagaEvent.REJEITAR_ANTIFRAUDE, SagaStatus.ANTIFRAUD_REJECTED),
                Arguments.of(SagaStatus.ANTIFRAUD_REJECTED, SagaEvent.FALHAR, SagaStatus.FAILED),
                Arguments.of(SagaStatus.RESERVED, SagaEvent.FALHAR, SagaStatus.FAILED),
                Arguments.of(SagaStatus.DEBITED, SagaEvent.FALHAR, SagaStatus.FAILED),
                Arguments.of(SagaStatus.CREDITED, SagaEvent.FALHAR, SagaStatus.FAILED),
                // Transições adicionadas para o cenário 3 (timeout): permitem falhar a saga a
                // partir de qualquer estado intermediário não-terminal, não só dos estados que o
                // fluxo feliz normalmente atravessa antes de uma rejeição/falha explícita.
                Arguments.of(SagaStatus.STARTED, SagaEvent.FALHAR, SagaStatus.FAILED),
                Arguments.of(SagaStatus.ANTIFRAUD_PENDING, SagaEvent.FALHAR, SagaStatus.FAILED),
                Arguments.of(SagaStatus.ANTIFRAUD_APPROVED, SagaEvent.FALHAR, SagaStatus.FAILED)
        );
    }

    /**
     * Cria uma saga e força seu estado inicial via transições reais da própria máquina de
     * estados (não por reflexão), garantindo que o caminho até o estadoAtual testado também é
     * um caminho válido segundo o diagrama.
     */
    private SagaInstance criarSagaNoEstado(SagaStatus estadoAtual) {
        SagaInstance saga = criarSaga();
        if (estadoAtual == SagaStatus.STARTED) {
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.RESERVAR_SALDO);
        if (estadoAtual == SagaStatus.RESERVED) {
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.SOLICITAR_ANTIFRAUDE);
        if (estadoAtual == SagaStatus.ANTIFRAUD_PENDING) {
            return saga;
        }
        if (estadoAtual == SagaStatus.ANTIFRAUD_REJECTED) {
            stateMachine.transicionar(saga, SagaEvent.REJEITAR_ANTIFRAUDE);
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.APROVAR_ANTIFRAUDE);
        if (estadoAtual == SagaStatus.ANTIFRAUD_APPROVED) {
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.DEBITAR);
        if (estadoAtual == SagaStatus.DEBITED) {
            return saga;
        }
        stateMachine.transicionar(saga, SagaEvent.CREDITAR);
        if (estadoAtual == SagaStatus.CREDITED) {
            return saga;
        }
        throw new IllegalArgumentException("Caminho de setup não definido para o estado " + estadoAtual);
    }
}
