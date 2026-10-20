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
 * Conduz a saga: cada método trata um evento recebido, aplica a transição na {@link SagaStateMachine} e publica o
 * próximo comando via {@link SagaOutboxPublisher}. Tudo é assíncrono via Kafka; abrir uma transferência é do
 * {@link TransferInitiationService}.
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
    // Domínio puro, sem @Component de propósito.
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

    /** A reserva confirmada deixa a checagem de fraude em andamento, então aplica as duas transições em sequência. */
    @Transactional
    public void processarReservaConfirmada(TransferReserved event) {
        SagaInstance saga = buscarSaga(event.transferId());
        transicionar(saga, SagaEvent.RESERVAR_SALDO, SagaEvent.SOLICITAR_ANTIFRAUDE);
        persistirEPublicarStatus(saga);
    }

    /*
     * Nada foi reservado, então não há o que compensar: a saga falha já com o motivo real. Ignora o evento se a
     * saga saiu de STARTED (o timeout já a falhou, ou houve reentrega), para não mandar uma mensagem legítima
     * à DLQ por transição inválida.
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
     * Compensação 1: antifraude rejeitou. Só a reserva precisa ser desfeita; a saga fica em ANTIFRAUD_REJECTED até
     * o account-service confirmar a liberação (ReservationReleased).
     */
    @Transactional
    public void processarAntifraudeRejeitada(AntiFraudRejected event) {
        SagaInstance saga = buscarSaga(event.transferId());
        transicionar(saga, SagaEvent.REJEITAR_ANTIFRAUDE);
        compensar(saga, event.motivo(), outboxPublisher::liberacaoDeReservaSolicitada);
    }

    /*
     * Compensação 2: o débito já foi confirmado e o crédito falhou. A saga só vira FAILED quando o account-service
     * confirmar a reversão (DebitReversed); o motivo é registrado já agora para ficar visível durante a compensação.
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
     * Chamado pelo SagaTimeoutScheduler para sagas travadas em estado não-terminal. A compensação depende de quanto
     * a saga avançou e reaproveita os comandos das compensações 1 e 2.
     */
    @Transactional
    public void processarTimeout(UUID transferId) {
        SagaInstance saga = buscarSaga(transferId);
        switch (saga.getStatus()) {
            case STARTED -> {
                // Nada foi reservado: não há o que compensar.
                saga.registrarMotivoFalha(MOTIVO_TIMEOUT);
                falharSaga(saga);
            }
            case RESERVED, ANTIFRAUD_PENDING, ANTIFRAUD_APPROVED ->
                    compensar(saga, MOTIVO_TIMEOUT, outboxPublisher::liberacaoDeReservaSolicitada);
            case DEBITED -> compensar(saga, MOTIVO_TIMEOUT, outboxPublisher::reversaoDeDebitoSolicitada);
            default -> {
                // CREDITED é transitório e COMPLETED/FAILED são terminais: nada a compensar.
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
