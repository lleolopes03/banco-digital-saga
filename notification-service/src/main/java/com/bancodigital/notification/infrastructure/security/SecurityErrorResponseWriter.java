package com.bancodigital.notification.infrastructure.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

/*
 * Corpo de erro consistente com o de um @RestControllerAdvice ({timestamp,status,error,message}),
 * usado pelos pontos de falha de autenticação/autorização que rodam antes do DispatcherServlet
 * (filtro JWT, entry point, access denied handler) e por isso não podem passar por um advice.
 */
final class SecurityErrorResponseWriter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private SecurityErrorResponseWriter() {
    }

    static void write(HttpServletResponse response, HttpStatus status, String message) throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", status.getReasonPhrase());
        body.put("message", message);
        response.getWriter().write(OBJECT_MAPPER.writeValueAsString(body));
    }
}
