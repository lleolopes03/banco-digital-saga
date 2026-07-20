package com.bancodigital.account.application;

import com.bancodigital.account.domain.InvalidCredentialsException;
import com.bancodigital.account.domain.User;
import com.bancodigital.account.infrastructure.persistence.UserRepository;
import com.bancodigital.account.infrastructure.security.JwtService;
import com.bancodigital.account.infrastructure.security.TokenBlacklistService;
import com.bancodigital.account.web.dto.LoginRequest;
import com.bancodigital.account.web.dto.LoginResponse;
import io.jsonwebtoken.Claims;
import java.time.Duration;
import java.time.Instant;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final TokenBlacklistService tokenBlacklistService;

    public AuthService(UserRepository userRepository, PasswordEncoder passwordEncoder, JwtService jwtService,
                        TokenBlacklistService tokenBlacklistService) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtService = jwtService;
        this.tokenBlacklistService = tokenBlacklistService;
    }

    public LoginResponse login(LoginRequest request) {
        User user = userRepository.findByEmail(request.email())
                .filter(candidate -> passwordEncoder.matches(request.senha(), candidate.getPasswordHash()))
                .orElseThrow(InvalidCredentialsException::new);

        String token = jwtService.gerarToken(user.getId(), user.getAccountId());
        return new LoginResponse(token, user.getAccountId(), jwtService.expiration().toSeconds());
    }

    /*
     * Blacklist só o JTI, com TTL igual ao tempo de validade restante do token: assim a entrada
     * some do Redis sozinha no exato momento em que o token expiraria de qualquer forma, sem
     * precisar de faxina manual.
     */
    public void logout(String token) {
        Claims claims = jwtService.validarEExtrairClaims(token);
        Duration ttlRestante = Duration.between(Instant.now(), claims.getExpiration().toInstant());
        tokenBlacklistService.blacklist(claims.getId(), ttlRestante);
    }
}
