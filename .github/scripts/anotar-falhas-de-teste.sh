#!/usr/bin/env bash
# Publica como anotação do GitHub o nome e o stack trace de cada teste que falhou (e as linhas
# [ERROR] do Maven), para o motivo aparecer no resumo do job sem abrir um log de 100 mil linhas.
# Uso: anotar-falhas-de-teste.sh <diretorio-do-servico>
set -u
servico="$1"

# Workflow commands usam %, \r e \n como separadores: escapa para a mensagem sair inteira.
escapar() { sed ':a;N;$!ba;s/%/%25/g;s/\r//g;s/\n/%0A/g'; }

for relatorio in "$servico"/target/surefire-reports/*.txt "$servico"/target/failsafe-reports/*.txt; do
  [ -f "$relatorio" ] || continue
  grep -qE '<<< (FAILURE|ERROR)' "$relatorio" || continue
  detalhe=$(head -c 3500 "$relatorio" | escapar)
  echo "::error title=Teste falhou - $(basename "$relatorio" .txt)::${detalhe}"
done

# Cobre também falhas sem relatório de teste (compilação, dependência, JVM que morreu).
if [ -f "$servico/mvn.log" ]; then
  resumo=$(grep -E '^\[ERROR\]|BUILD FAILURE|Tests run:.*(Failures|Errors): [1-9]' "$servico/mvn.log" | head -c 3500 | escapar)
  [ -n "$resumo" ] && echo "::error title=Resumo do Maven - $servico::${resumo}"
fi
exit 0