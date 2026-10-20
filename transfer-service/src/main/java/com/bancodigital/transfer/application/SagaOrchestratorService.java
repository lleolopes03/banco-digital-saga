package com.bancodigital.transfer.application;

import com.bancodigital.transfer.domain.SagaEvent;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaInstanceNotFoundException;
import com.bancodigital.transfer.domain.SagaStateMachine;
import com.bancodigital.transfer.domain.SagaStatus;
import com.bancodigital.transfer.domain.event.AntiFraudApproved;
import com.bancodigital.transfer.domain.event.AntiFraudRejected;
import com.bancodigital.transfer.domain.event.CreditConfirmed;
import com.bancodigital.transfer.domain.event.CreditFailed;
import com.bancodigital.transfer.domain.event.DebitConfirmed;
import com.bancodigital.transfer.domain.event.DebitReversed;
import com.bancodigital.transfer.domain.event.ReservationReleased;
import com.bancodigital.transfer.domain.event.TransferReservationFailed;
import com.bancodigital.transfer.domain.event.TransferReserved;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import com.bancodigital.transfer.infrastructure.websocket.SagaStatusPublisher;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Orquestra a saga de transferência: cada método corresponde a um evento recebido e aplica a
 * transição correspondente via {@link SagaStateMachine}, publicando (via {@link SagaOutboxPublisher})
 * o próximo comando da saga quando necessário. Nenhum método aqui chama outro serviço diretamente —
 * tudo é assíncrono via Kafka. Abrir uma transferência é do {@link TransferInitiationService}.
 */
@Service
public class SagaOrchestratorService {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestratorService.class);
    private static final String MOTIVO_TIMEOUT = "SAGA_TIMEOUT";
    private static final String RESULTADO_FALHA = "FAILED";
    private static final String RESULTADO_SUCESSO = "COMPLETED";

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaOutboxPublisher outboxPublisher;
    private final SagaObservability sagaObservability;
    private final SagaStatusPublisher sagaStatusPublisher;
    // SagaStateMachine é domínio puro, sem @Component de propósito, então instanciamos direto em vez
    // de injetar via Spring.
    private final SagaStateMachine stateMachine = new SagaStateMachine();

    public SagaOrchestratorService(SagaInstanceRepository sagaInstanceRepository,
                                    SagaOutboxPublisher outboxPublisher,
                                    SagaObservability sagaObservability,
                                    SagaStatusPublisher sagaStatusPublisher) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.outboxPublisher = outboxPublisher;
        this.sagaObservability = sagaObservability;
        this.sagaStatusPublisher = sagaStatusPublisher;
    }

    /*
     * A reserva de saldo é o primeiro passo de negócio da saga (antes de qualquer checagem de
     * fraude). Quando o account-service confirma que reservou o saldo, a checagem de fraude — que o
     * antifraud-service já processa de forma independente, escutando o mesmo evento — está em
     * andamento; por isso aplicamos as duas transições (RESERVED e ANTIFRAUD_PENDING) em sequência.
     */
    @Transactional
    public void processarReservaConfirmada(TransferReserved event) {
        SagaInstance saga = buscarSaga(event.transferId());
        transicionar(saga, SagaEvent.RESERVAR_SALDO, SagaEvent.SOLICITAR_ANTIFRAUDE);
        persistirEPublicarStatus(saga);
    }

    /*
     * A reserva falhou (hoje: SALDO_INSUFICIENTE): nenhum dinheiro foi reservado, então não há o que
     * compensar — a saga termina em FAILED já com o motivo REAL, e o usuário é avisado via
     * TransferFailed. Só age sobre uma saga ainda em STARTED: se o timeout já a falhou (ou o evento
     * foi reentregue), o evento é ignorado em vez de lançar InvalidSagaTransitionException, que
     * mandaria uma mensagem perfeitamente legítima para a DLQ.
     */
    @Transactional
    public void processarReservaFalhou(TransferReservationFailed event) {
        SagaInstance saga = buscarSaga(event.transferId());
        if (saga.getStatus() != SagaStatus.STARTED) {
            log.info("TransferReservationFailed ignorado para a saga {}: status atual {} (esperado STARTED)",
                    saga.getId(), saga.getStatus());
            return;
        }
        saga.registrarMotivoFalha(event.motivo());
        falharSaga(saga);
    }

    @Transactional
    public void processarAntifraudeAprovada(AntiFraudApproved event) {
        SagaInstance saga = buscarSaga(event.transferId());
        transicionar(saga, SagaEvent.APROVAR_ANTIFRAUDE);
        persistirEPublicarStatus(saga);
        outboxPublisher.debitoSolicitado(saga);
    }

    @Transactional
    public void processarDebitoConfirmado(DebitConfirmed event) {
        SagaInstance saga = buscarSaga(event.transferId());
        transicionar(saga, SagaEvent.DEBITAR);
        persistirEPublicarStatus(saga);
        outboxPublisher.creditoSolicitado(saga);
    }

    @Transactional
    public void processarCreditoConfirmado(CreditConfirmed event) {
        SagaInstance saga = buscarSaga(event.transferId());
        transicionar(saga, SagaEvent.CREDITAR, SagaEvent.COMPLETAR);
        persistirEPublicarStatus(saga);
        sagaObservability.registrarSagaFinalizada(RESULTADO_SUCESSO);
        outboxPublisher.transferenciaConcluida(saga);
    }

    /*
     * Cenário 1 de compensação: antifraude rejeitou a transferência. A saga já tinha reservado saldo
     * mas ainda não confirmou débito nenhum, então a única coisa a desfazer é a reserva. Vai para
     * ANTIFRAUD_REJECTED (não direto para FAILED) porque o dinheiro só volta de fato quando o
     * account-service confirmar a liberação via ReservationReleased — a saga permanece observável
     * nesse estado intermediário até lá.
     */
    @Transactional
    public void processarAntifraudeRejeitada(AntiFraudRejected event) {
        SagaInstance saga = buscarSaga(event.transferId());
        transicionar(saga, SagaEvent.REJEITAR_ANTIFRAUDE);
        compensar(saga, event.motivo(), outboxPublisher::liberacaoDeReservaSolicitada);
    }

    /*
     * Cenário 2 de compensação: o débito na origem já foi confirmado (saga em DEBITED), mas o crédito
     * no destino falhou (conta bloqueada/fechada). Diferente da rejeição por antifraude, aqui não há
     * transição imediata — a saga só vira FAILED quando o account-service confirmar a reversão do
     * débito via DebitReversed. O motivo é registrado já agora para ficar visível enquanto a
     * compensação ainda está em voo.
     */
    @Transactional
    public void processarCreditoFalhou(CreditFailed event) {
        SagaInstance saga = buscarSaga(event.transferId());
        compensar(saga, event.motivo(), outboxPublisher::reversaoDeDebitoSolicitada);
    }

    @Transactional
    public void processarReservaLiberada(ReservationReleased event) {
        falharSaga(buscarSaga(event.transferId()));
    }

    @Transactional
    public void processarDebitoRevertido(DebitReversed event) {
        falharSaga(buscarSaga(event.transferId()));
    }

    /*
     * Cenário 3: o SagaTimeoutScheduler chama este método para sagas travadas há mais de 2 minutos
     * em um estado não-terminal. A compensação depende de quanto a saga avançou antes de travar e
     * reaproveita os mesmos comandos dos cenários 1 e 2, só que disparados por timeout em vez de por
     * um evento explícito de rejeição/falha.
     */
    @Transactional
    public void processarTimeout(UUID transferId) {
        SagaInstance saga = buscarSaga(transferId);
        switch (saga.getStatus()) {
            case STARTED -> {
                // Nenhum dinheiro chegou a ser reservado (o account-service nunca processou o
                // TransferRequested): não há o que compensar, só falhar a saga.
                saga.registrarMotivoFalha(MOTIVO_TIMEOUT);
                falharSaga(saga);
            }
            case RESERVED, ANTIFRAUD_PENDING, ANTIFRAUD_APPROVED ->
                    compensar(saga, MOTIVO_TIMEOUT, outboxPublisher::liberacaoDeReservaSolicitada);
            case DEBITED -> compensar(saga, MOTIVO_TIMEOUT, outboxPublisher::reversaoDeDebitoSolicitada);
            default -> {
                // CREDITED é transitório (a mesma transação que credita já completa a saga em
                // seguida) e COMPLETED/FAILED são terminais — o scheduler não deveria chamar este
                // método para eles, mas se chamar, não há ação de compensação a tomar.
            }
        }
    }

    private SagaInstance buscarSaga(UUID transferId) {
        return sagaInstanceRepository.findById(transferId)
                .orElseThrow(() -> new SagaInstanceNotFoundException(transferId));
    }

    private void transicionar(SagaInstance saga, SagaEvent... eventos) {
        for (SagaEvent evento : eventos) {
            stateMachine.transicionar(saga, evento);
        }
    }

    /** Persiste o novo estado e o publica para quem acompanha a saga em tempo real (WebSocket). */
    private void persistirEPublicarStatus(SagaInstance saga) {
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);
    }

    /** Termina a saga em FAILED e avisa o usuário (TransferFailed). */
    private void falharSaga(SagaInstance saga) {
        transicionar(saga, SagaEvent.FALHAR);
        persistirEPublicarStatus(saga);
        sagaObservability.registrarSagaFinalizada(RESULTADO_FALHA);
        outboxPublisher.transferenciaFalhou(saga);
    }

    /** Registra o motivo e dispara o comando que desfaz o que a saga já tinha feito. */
    private void compensar(SagaInstance saga, String motivo, Consumer<SagaInstance> comandoDeCompensacao) {
        saga.registrarMotivoFalha(motivo);
        persistirEPublicarStatus(saga);
        sagaObservability.registrarCompensacao(motivo);
        comandoDeCompensacao.accept(saga);
    }
}
