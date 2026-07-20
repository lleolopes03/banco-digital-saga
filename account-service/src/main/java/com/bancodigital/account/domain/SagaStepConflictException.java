package com.bancodigital.account.domain;

import java.util.UUID;

/**
 * Lançada quando um passo da saga de transferência (reserva, confirmação de débito ou
 * crédito) esgota as tentativas de retry por conflito de lock otimista na conta. Diferente de
 * ConcurrentDepositConflictException, não é mapeada para uma resposta HTTP: esses passos são
 * disparados por consumers Kafka, não por chamadas síncronas, então a falha propaga para o
 * container do listener tratar (log + possível redelivery).
 */
public class SagaStepConflictException extends RuntimeException {

    public SagaStepConflictException(String etapa, UUID accountId) {
        super("Não foi possível concluir a etapa '" + etapa + "' na conta " + accountId
                + " após múltiplas tentativas: alta concorrência de atualizações de saldo");
    }
}
