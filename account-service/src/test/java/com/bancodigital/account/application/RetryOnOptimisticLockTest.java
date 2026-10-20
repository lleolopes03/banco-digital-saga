package com.bancodigital.account.application;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Recover;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/*
 * Garante que a meta-anotação @RetryOnOptimisticLock realmente aciona o retry do spring-retry. Se
 * ela deixasse de ser reconhecida, o código continuaria compilando e os testes unitários (que não
 * têm proxy AOP) passariam, mas conflitos de lock otimista deixariam de ser reexecutados.
 */
@SpringJUnitConfig(RetryOnOptimisticLockTest.Config.class)
class RetryOnOptimisticLockTest {

    @Autowired
    private OperacaoConcorrente operacao;

    @BeforeEach
    void zerar() {
        operacao.zerar();
    }

    @Test
    void deveReexecutarAteConseguirQuandoHaConflitoDeLockOtimista() {
        operacao.falharPrimeiras(2);

        String resultado = operacao.executar();

        assertThat(resultado).isEqualTo("ok");
        assertThat(operacao.chamadas()).isEqualTo(3);
    }

    @Test
    void deveChamarORecoverDepoisDeEsgotarAsTresTentativas() {
        operacao.falharPrimeiras(Integer.MAX_VALUE);

        String resultado = operacao.executar();

        assertThat(resultado).isEqualTo("recuperado");
        assertThat(operacao.chamadas()).isEqualTo(3);
    }

    @Test
    void naoDeveReexecutarQuandoNaoHaConflito() {
        assertThat(operacao.executar()).isEqualTo("ok");
        assertThat(operacao.chamadas()).isEqualTo(1);
    }

    @Configuration
    @EnableRetry
    static class Config {

        @Bean
        OperacaoConcorrente operacaoConcorrente() {
            return new OperacaoConcorrente();
        }
    }

    static class OperacaoConcorrente {

        private int chamadas;
        private int falhasRestantes;

        @RetryOnOptimisticLock
        public String executar() {
            chamadas++;
            if (falhasRestantes > 0) {
                falhasRestantes--;
                throw new OptimisticLockingFailureException("conflito simulado");
            }
            return "ok";
        }

        @Recover
        public String recuperar(OptimisticLockingFailureException ex) {
            return "recuperado";
        }

        public void falharPrimeiras(int vezes) {
            falhasRestantes = vezes;
        }

        public int chamadas() {
            return chamadas;
        }

        public void zerar() {
            chamadas = 0;
            falhasRestantes = 0;
        }
    }
}
