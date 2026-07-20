package com.bancodigital.antifraud.infrastructure.persistence;

import com.bancodigital.antifraud.domain.AntiFraudCheck;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AntiFraudCheckRepository extends JpaRepository<AntiFraudCheck, UUID> {
}
