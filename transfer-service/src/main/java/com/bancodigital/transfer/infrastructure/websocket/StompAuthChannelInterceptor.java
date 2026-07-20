package com.bancodigital.transfer.infrastructure.websocket;

import com.bancodigital.transfer.domain.SagaInstance;
import com.bancodigital.transfer.infrastructure.persistence.SagaInstanceRepository;
import com.bancodigital.transfer.infrastructure.security.JwtService;
import com.bancodigital.transfer.infrastructure.security.TokenBlacklistService;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import java.util.UUID;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;

/*
 * A conexão HTTP de handshake do WebSocket (o GET inicial em /ws) é liberada em permitAll() no
 * SecurityConfig, porque um WebSocket nativo do browser não permite anexar um header
 * Authorization nesse handshake. A autenticação de verdade acontece aqui, no frame STOMP CONNECT
 * — o cliente Angular manda o JWT em connectHeaders (Authorization: Bearer ...), que chega como
 * header STOMP nativo, não como header HTTP.
 *
 * Além de autenticar, este interceptor autoriza cada SUBSCRIBE: só o dono da conta de origem ou
 * de destino da transferência pode assinar /topic/transfers/{transferId}, evitando que qualquer
 * usuário autenticado espie o progresso de uma saga alheia (o GET /api/transfers/{id} REST já
 * tem essa mesma lacuna hoje, mas aqui não repetimos o descuido).
 */
@Component
public class StompAuthChannelInterceptor implements ChannelInterceptor {

    private static final String AUTH_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String TRANSFERS_TOPIC_PREFIX = "/topic/transfers/";

    private final JwtService jwtService;
    private final TokenBlacklistService tokenBlacklistService;
    private final SagaInstanceRepository sagaInstanceRepository;

    public StompAuthChannelInterceptor(JwtService jwtService, TokenBlacklistService tokenBlacklistService,
                                        SagaInstanceRepository sagaInstanceRepository) {
        this.jwtService = jwtService;
        this.tokenBlacklistService = tokenBlacklistService;
        this.sagaInstanceRepository = sagaInstanceRepository;
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        // MessageHeaderAccessor.getAccessor (não StompHeaderAccessor.wrap) é essencial aqui: ele
        // recupera o accessor MUTÁVEL já em cache nos headers da mensagem (colocado lá pelo
        // StompSubProtocolHandler do Spring). Mutar esse accessor — accessor.setUser(...) — reflete
        // na mensagem que segue adiante no channel. wrap() cria um accessor novo e desconectado;
        // setUser() nele some no vácuo, e o Principal do CONNECT nunca chega ao SUBSCRIBE seguinte
        // na mesma sessão (foi assim que essa autorização ficou quebrada na primeira versão).
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);

        if (StompCommand.CONNECT.equals(accessor.getCommand())) {
            accessor.setUser(autenticar(accessor));
        } else if (StompCommand.SUBSCRIBE.equals(accessor.getCommand())) {
            autorizarSubscricao(accessor);
        }

        return message;
    }

    private StompPrincipal autenticar(StompHeaderAccessor accessor) {
        String header = accessor.getFirstNativeHeader(AUTH_HEADER);
        if (header == null || !header.startsWith(BEARER_PREFIX)) {
            throw new MessagingException("Token JWT ausente no CONNECT.");
        }

        String token = header.substring(BEARER_PREFIX.length());
        try {
            Claims claims = jwtService.validarEExtrairClaims(token);
            if (tokenBlacklistService.estaNaBlacklist(claims.getId())) {
                throw new MessagingException("Token invalidado por logout.");
            }
            return new StompPrincipal(jwtService.extrairAccountId(claims));
        } catch (JwtException | IllegalArgumentException ex) {
            throw new MessagingException("Token JWT inválido ou expirado.", ex);
        }
    }

    private void autorizarSubscricao(StompHeaderAccessor accessor) {
        String destination = accessor.getDestination();
        if (destination == null || !destination.startsWith(TRANSFERS_TOPIC_PREFIX)) {
            return;
        }

        if (!(accessor.getUser() instanceof StompPrincipal principal)) {
            throw new MessagingException("Assinatura recusada: usuário não autenticado.");
        }

        UUID transferId = UUID.fromString(destination.substring(TRANSFERS_TOPIC_PREFIX.length()));
        SagaInstance saga = sagaInstanceRepository.findById(transferId)
                .orElseThrow(() -> new MessagingException("Transferência não encontrada: " + transferId));

        UUID accountId = principal.accountId();
        if (!accountId.equals(saga.getAccountOrigemId()) && !accountId.equals(saga.getAccountDestinoId())) {
            throw new MessagingException("Assinatura recusada: transferência não pertence ao usuário autenticado.");
        }
    }
}
