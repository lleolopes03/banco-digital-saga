package com.bancodigital.transfer.application;

import com.bancodigital.transfer.domain.InsufficientBalanceException;
import com.bancodigital.transfer.domain.OutboxEvent;
import com.bancodigital.transfer.domain.SagaEvent;
import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.domain.SagaInstanceNotFoundException;
import com.bancodigital.transfer.domain.SagaStateMachine;
import com.bancodigital.transfer.domain.event.AntiFraudApproved;
import com.bancodigital.transfer.domain.event.AntiFraudRejected;
import com.bancodigital.transfer.domain.event.CreditConfirmed;
import com.bancodigital.transfer.domain.event.CreditFailed;
import com.bancodigital.transfer.domain.event.CreditRequested;
import com.bancodigital.transfer.domain.event.DebitConfirmationRequested;
import com.bancodigital.transfer.domain.event.DebitConfirmed;
import com.bancodigital.transfer.domain.event.DebitReversed;
import com.bancodigital.transfer.domain.event.ReleaseReservedAmount;
import com.bancodigital.transfer.domain.event.ReservationReleased;
import com.bancodigital.transfer.domain.event.ReverseDebitRequested;
import com.bancodigital.transfer.domain.event.TransferRequested;
import com.bancodigital.transfer.domain.event.TransferReserved;
import com.bancodigital.transfer.infrastructure.client.AccountServiceClient;
import com.bancodigital.transfer.infrastructure.client.BalanceResponse;
import com.bancodigital.transfer.infrastructure.observability.SagaObservability;
import com.bancodigital.transfer.infrastructure.persistence.OutboxEventRepository;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import com.bancodigital.transfer.infrastructure.websocket.SagaStatusPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Orquestra a saga de transferência: cada método corresponde a um evento recebido (ou à
 * criação inicial) e aplica a transição correspondente via {@link SagaStateMachine}, publicando
 * (via Outbox) o próximo comando da saga quando necessário. Nenhum método aqui chama outro
 * serviço diretamente — tudo é assíncrono via Kafka.
 */
@Service
public class SagaOrchestratorService {

    private static final String AGGREGATE_TYPE_TRANSFER = "Transfer";
    private static final String MOTIVO_TIMEOUT = "SAGA_TIMEOUT";
    private static final ObjectMapper OUTBOX_PAYLOAD_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final SagaInstanceRepository sagaInstanceRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final AccountServiceClient accountServiceClient;
    private final SagaObservability sagaObservability;
    private final SagaStatusPublisher sagaStatusPublisher;
    // SagaStateMachine é domínio puro, sem @Component de propósito, então instanciamos direto
    // em vez de injetar via Spring.
    private final SagaStateMachine stateMachine = new SagaStateMachine();

    public SagaOrchestratorService(SagaInstanceRepository sagaInstanceRepository,
                                    OutboxEventRepository outboxEventRepository,
                                    AccountServiceClient accountServiceClient,
                                    SagaObservability sagaObservability,
                                    SagaStatusPublisher sagaStatusPublisher) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.accountServiceClient = accountServiceClient;
        this.sagaObservability = sagaObservability;
        this.sagaStatusPublisher = sagaStatusPublisher;
    }

    @Transactional
    public UUID iniciarTransferencia(UUID accountOrigemId, UUID accountDestinoId, BigDecimal valor,
                                      String idempotencyKey) {
        boolean temIdempotencyKey = idempotencyKey != null && !idempotencyKey.isBlank();
        if (temIdempotencyKey) {
            Optional<SagaInstance> existente = sagaInstanceRepository.findByIdempotencyKey(idempotencyKey);
            if (existente.isPresent()) {
                return existente.get().getId();
            }
        }

        validarSaldoPrevio(accountOrigemId, valor);

        UUID transferId = UUID.randomUUID();
        SagaInstance saga = SagaInstance.iniciar(transferId, accountOrigemId, accountDestinoId, valor,
                temIdempotencyKey ? idempotencyKey : null);

        try {
            // saveAndFlush (não save) para que uma violação do índice único parcial de
            // idempotency_key seja lançada já aqui dentro, e não só no commit da transação —
            // só assim dá pra capturá-la e cair no caminho de corrida abaixo.
            sagaInstanceRepository.saveAndFlush(saga);
        } catch (DataIntegrityViolationException ex) {
            if (!temIdempotencyKey) {
                throw ex;
            }
            // Corrida rara: duas requisições concorrentes com a mesma Idempotency-Key. O
            // índice único garantiu que só uma delas persistiu; devolvemos a saga que
            // realmente venceu em vez de propagar o erro.
            return sagaInstanceRepository.findByIdempotencyKey(idempotencyKey)
                    .map(SagaInstance::getId)
                    .orElseThrow(() -> ex);
        }

        TransferRequested payload = new TransferRequested(transferId, accountOrigemId, accountDestinoId, valor,
                Instant.now());
        publicarEvento(transferId, "TransferRequested", payload);

        return transferId;
    }

    /*
     * Validação síncrona e best-effort: evita abrir uma saga inteira para um caso óbvio de
     * saldo insuficiente. Se o account-service estiver indisponível, AccountServiceClient
     * degrada graciosamente (Optional vazio) e essa checagem é simplesmente pulada — a
     * validação definitiva de saldo continua sendo a reserva assíncrona feita mais abaixo na
     * saga, que independe desta chamada.
     */
    private void validarSaldoPrevio(UUID accountOrigemId, BigDecimal valor) {
        Optional<BalanceResponse> saldo = accountServiceClient.consultarSaldo(accountOrigemId);
        if (saldo.isPresent() && saldo.get().balance().compareTo(valor) < 0) {
            throw new InsufficientBalanceException(accountOrigemId, valor, saldo.get().balance());
        }
    }

    /*
     * A reserva de saldo é o primeiro passo de negócio da saga (antes de qualquer checagem de
     * fraude). Quando o account-service confirma que reservou o saldo, isso significa que a
     * checagem de fraude — que o antifraud-service já está processando de forma independente,
     * escutando o mesmo evento — está em andamento; por isso aplicamos as duas transições
     * (RESERVED e ANTIFRAUD_PENDING) em sequência aqui.
     */
    @Transactional
    public void processarReservaConfirmada(TransferReserved event) {
        SagaInstance saga = buscarSaga(event.transferId());
        stateMachine.transicionar(saga, SagaEvent.RESERVAR_SALDO);
        stateMachine.transicionar(saga, SagaEvent.SOLICITAR_ANTIFRAUDE);
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);
    }

    @Transactional
    public void processarAntifraudeAprovada(AntiFraudApproved event) {
        SagaInstance saga = buscarSaga(event.transferId());
        stateMachine.transicionar(saga, SagaEvent.APROVAR_ANTIFRAUDE);
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);

        DebitConfirmationRequested payload = new DebitConfirmationRequested(saga.getId(), saga.getAccountOrigemId(),
                saga.getValor(), Instant.now());
        publicarEvento(saga.getId(), "DebitConfirmationRequested", payload);
    }

    @Transactional
    public void processarDebitoConfirmado(DebitConfirmed event) {
        SagaInstance saga = buscarSaga(event.transferId());
        stateMachine.transicionar(saga, SagaEvent.DEBITAR);
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);

        CreditRequested payload = new CreditRequested(saga.getId(), saga.getAccountDestinoId(), saga.getValor(),
                Instant.now());
        publicarEvento(saga.getId(), "CreditRequested", payload);
    }

    @Transactional
    public void processarCreditoConfirmado(CreditConfirmed event) {
        SagaInstance saga = buscarSaga(event.transferId());
        stateMachine.transicionar(saga, SagaEvent.CREDITAR);
        stateMachine.transicionar(saga, SagaEvent.COMPLETAR);
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);
        sagaObservability.registrarSagaFinalizada("COMPLETED");
    }

    /*
     * Cenário 1 de compensação: antifraude rejeitou a transferência. A saga já tinha reservado
     * saldo (RESERVED/ANTIFRAUD_PENDING) mas ainda não confirmou débito nenhum, então a única
     * coisa a desfazer é a reserva. Vai para ANTIFRAUD_REJECTED (não direto para FAILED) porque
     * o dinheiro só volta de fato quando o account-service confirmar a liberação via
     * ReservationReleased — a saga permanece observável nesse estado intermediário até lá.
     */
    @Transactional
    public void processarAntifraudeRejeitada(AntiFraudRejected event) {
        SagaInstance saga = buscarSaga(event.transferId());
        stateMachine.transicionar(saga, SagaEvent.REJEITAR_ANTIFRAUDE);
        saga.registrarMotivoFalha(event.motivo());
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);
        sagaObservability.registrarCompensacao(event.motivo());

        publicarLiberacaoReserva(saga);
    }

    /*
     * Cenário 2 de compensação: o débito na origem já foi confirmado (saga em DEBITED), mas o
     * crédito no destino falhou (conta bloqueada/fechada). Diferente da rejeição por antifraude,
     * aqui não há uma transição de estado imediata — a saga só vira FAILED quando o
     * account-service confirmar a reversão do débito via DebitReversed. O failureReason é
     * registrado já nesta etapa para que o motivo fique visível mesmo enquanto a compensação
     * ainda está em voo.
     */
    @Transactional
    public void processarCreditoFalhou(CreditFailed event) {
        SagaInstance saga = buscarSaga(event.transferId());
        saga.registrarMotivoFalha(event.motivo());
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);
        sagaObservability.registrarCompensacao(event.motivo());

        publicarReversaoDebito(saga);
    }

    @Transactional
    public void processarReservaLiberada(ReservationReleased event) {
        SagaInstance saga = buscarSaga(event.transferId());
        stateMachine.transicionar(saga, SagaEvent.FALHAR);
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);
        sagaObservability.registrarSagaFinalizada("FAILED");
    }

    @Transactional
    public void processarDebitoRevertido(DebitReversed event) {
        SagaInstance saga = buscarSaga(event.transferId());
        stateMachine.transicionar(saga, SagaEvent.FALHAR);
        sagaInstanceRepository.save(saga);
        sagaStatusPublisher.publish(saga);
        sagaObservability.registrarSagaFinalizada("FAILED");
    }

    /*
     * Cenário 3: o SagaTimeoutScheduler chama este método para sagas travadas há mais de 2
     * minutos em um estado não-terminal. A compensação disparada depende de quanto da saga já
     * tinha avançado quando ela travou: reaproveita exatamente os mesmos comandos de
     * compensação dos cenários 1 e 2 (publicarLiberacaoReserva/publicarReversaoDebito), só que
     * disparados por timeout em vez de por um evento de rejeição/falha explícito.
     */
    @Transactional
    public void processarTimeout(UUID transferId) {
        SagaInstance saga = buscarSaga(transferId);
        switch (saga.getStatus()) {
            case STARTED -> {
                // Nenhum dinheiro chegou a ser reservado ainda (o account-service nunca
                // processou o TransferRequested) — não há o que compensar, só falhar a saga.
                saga.registrarMotivoFalha(MOTIVO_TIMEOUT);
                stateMachine.transicionar(saga, SagaEvent.FALHAR);
                sagaInstanceRepository.save(saga);
                sagaStatusPublisher.publish(saga);
                sagaObservability.registrarSagaFinalizada("FAILED");
            }
            case RESERVED, ANTIFRAUD_PENDING, ANTIFRAUD_APPROVED -> {
                saga.registrarMotivoFalha(MOTIVO_TIMEOUT);
                sagaInstanceRepository.save(saga);
                sagaStatusPublisher.publish(saga);
                sagaObservability.registrarCompensacao(MOTIVO_TIMEOUT);
                publicarLiberacaoReserva(saga);
            }
            case DEBITED -> {
                saga.registrarMotivoFalha(MOTIVO_TIMEOUT);
                sagaInstanceRepository.save(saga);
                sagaStatusPublisher.publish(saga);
                sagaObservability.registrarCompensacao(MOTIVO_TIMEOUT);
                publicarReversaoDebito(saga);
            }
            default -> {
                // CREDITED é transitório (a mesma transação que credita já completa a saga em
                // seguida) e COMPLETED/FAILED são terminais — o scheduler não deveria chamar
                // este método para eles, mas se chamar, não há ação de compensação a tomar.
            }
        }
    }

    @Transactional(readOnly = true)
    public SagaInstance consultarSaga(UUID transferId) {
        return buscarSaga(transferId);
    }

    private SagaInstance buscarSaga(UUID transferId) {
        return sagaInstanceRepository.findById(transferId)
                .orElseThrow(() -> new SagaInstanceNotFoundException(transferId));
    }

    private void publicarLiberacaoReserva(SagaInstance saga) {
        ReleaseReservedAmount payload = new ReleaseReservedAmount(saga.getId(), saga.getAccountOrigemId(),
                saga.getValor(), Instant.now());
        publicarEvento(saga.getId(), "ReleaseReservedAmount", payload);
    }

    private void publicarReversaoDebito(SagaInstance saga) {
        ReverseDebitRequested payload = new ReverseDebitRequested(saga.getId(), saga.getAccountOrigemId(),
                saga.getValor(), Instant.now());
        publicarEvento(saga.getId(), "ReverseDebitRequested", payload);
    }

    /*
     * Captura o traceparent do trace ATIVO neste exato momento — o span HTTP de
     * iniciarTransferencia, ou o span aberto pelo consumer que disparou este método de negócio —
     * e grava junto do OutboxEvent, para que o OutboxPublisherScheduler propague o mesmo trace
     * para o próximo hop via Kafka.
     */
    private void publicarEvento(UUID transferId, String eventType, Object payload) {
        OutboxEvent outboxEvent = OutboxEvent.pending(UUID.randomUUID(), AGGREGATE_TYPE_TRANSFER, transferId,
                eventType, serializePayload(payload), sagaObservability.traceparentAtual());
        outboxEventRepository.save(outboxEvent);
    }

    private String serializePayload(Object event) {
        try {
            return OUTBOX_PAYLOAD_MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Falha ao serializar payload do outbox event", e);
        }
    }
}
