package com.bancodigital.antifraud.infrastructure.persistence;

import com.bancodigital.antifraud.domain.BlacklistedAccount;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BlacklistedAccountRepository extends JpaRepository<BlacklistedAccount, UUID> {
}
