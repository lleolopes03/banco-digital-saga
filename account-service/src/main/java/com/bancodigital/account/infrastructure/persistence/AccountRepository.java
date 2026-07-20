package com.bancodigital.account.infrastructure.persistence;

import com.bancodigital.account.domain.Account;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AccountRepository extends JpaRepository<Account, UUID> {

    Optional<Account> findByOwnerDocument(String ownerDocument);
}
