package com.bancodigital.account.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.function.Function;
import org.springframework.data.domain.Page;

@Schema(description = "Página de resultados.")
public record PageResponse<T>(
        @Schema(description = "Itens da página.") List<T> conteudo,
        @Schema(description = "Número da página atual, começando em 0.", example = "0") int paginaAtual,
        @Schema(description = "Total de páginas.", example = "3") int totalPaginas,
        @Schema(description = "Total de itens em todas as páginas.", example = "42") long totalElementos,
        @Schema(description = "Itens por página.", example = "20") int tamanhoPagina,
        @Schema(description = "Indica a primeira página.") boolean primeira,
        @Schema(description = "Indica a última página.") boolean ultima
) {

    public static <S, T> PageResponse<T> from(Page<S> page, Function<S, T> mapper) {
        return new PageResponse<>(
                page.getContent().stream().map(mapper).toList(),
                page.getNumber(),
                page.getTotalPages(),
                page.getTotalElements(),
                page.getSize(),
                page.isFirst(),
                page.isLast()
        );
    }
}
