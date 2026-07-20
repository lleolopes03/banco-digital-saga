package com.bancodigital.transfer.infrastructure.websocket;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/*
 * Broker STOMP simples (in-memory, não um broker externo como RabbitMQ) — suficiente aqui porque
 * cada mensagem é só um broadcast best-effort do progresso da saga, não algo que precise
 * sobreviver a um restart do transfer-service. O cliente Angular se conecta em /ws e assina
 * /topic/transfers/{transferId} para acompanhar uma saga específica.
 *
 * setAllowedOriginPatterns("*") porque este serviço não tem CORS configurado em lugar nenhum
 * (mesma convenção do resto do projeto — o frontend fala com os serviços via proxy do
 * `ng serve`, não direto), e um handshake de WebSocket cross-origin sem essa configuração é
 * rejeitado pelo Spring por padrão.
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final StompAuthChannelInterceptor stompAuthChannelInterceptor;

    public WebSocketConfig(StompAuthChannelInterceptor stompAuthChannelInterceptor) {
        this.stompAuthChannelInterceptor = stompAuthChannelInterceptor;
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws").setAllowedOriginPatterns("*");
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
        registry.setApplicationDestinationPrefixes("/app");
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(stompAuthChannelInterceptor);
    }
}
