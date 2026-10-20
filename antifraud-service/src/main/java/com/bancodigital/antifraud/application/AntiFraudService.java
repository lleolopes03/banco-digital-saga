package com.bancodigital.antifraud.application;

import com.bancodigital.antifraud.domain.AntiFraudCheck;
import com.bancodigital.antifraud.domain.event.AntiFraudApproved;
import com.bancodigital.antifraud.domain.event.AntiFraudRejected;
import com.bancodigital.antifraud.domain.event.TransferReserved;
import com.bancodigital.antifraud.infrastructure.persistence.AntiFraudCheckRepository;
import java.time.Instant;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Avalia uma transferência reservada, registra o resultado e publica a decisão via Outbox. */
@Service
public class AntiFraudService {

    private final AntiFraudRuleEngine ruleEngine;
    private final AntiFraudCheckRepository antiFraudCheckRepository;
    private final OutboxEventWriter outboxEventWriter;

    public AntiFraudService(AntiFraudRuleEngine ruleEngine, AntiFraudCheckRepository antiFraudCheckRepository,
                             OutboxEventWriter outboxEventWriter) {
        this.ruleEngine = ruleEngine;
        this.antiFraudCheckRepository = antiFraudCheckRepository;
        this.outboxEventWriter = outboxEventWriter;
    }

    @Transactional
    public void avaliar(TransferReserved event) {
        Optional<String> motivoRejeicao = ruleEngine.avaliar(event);
        if (motivoRejeicao.isPresent()) {
            rejeitar(event, motivoRejeicao.get());
        } else {
            aprovar(event);
        }
    }

    private void rejeitar(TransferReserved event, String motivo) {
        antiFraudCheckRepository.save(AntiFraudCheck.rejeitado(event.transferId(), motivo));
        outboxEventWriter.gravarParaTransferencia(event.transferId(),
                new AntiFraudRejected(event.transferId(), motivo, Instant.now()));
    }

    private void aprovar(TransferReserved event) {
        antiFraudCheckRepository.save(AntiFraudCheck.aprovado(event.transferId()));
        outboxEventWriter.gravarParaTransferencia(event.transferId(),
                new AntiFraudApproved(event.transferId(), Instant.now()));
    }
}
