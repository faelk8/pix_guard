# ADR 0006 — Transporte Kafka com captura durável na FASE 2

- Status: direção de captura Kafka em snapshots **aceita por Rafael em 2026-11-12**; publicação incremental autorizada. Cliente/versionamento, retenção, limiares e demais parâmetros permanecem **proposed**.
- Data: 2026-11-12
- Issue: #23 / PLAN-004
- Responsável: arquitetura de dados

## Contexto e fatos observados

O Compose declara Kafka 4.4.0 em KRaft single-node, `kafka:9092`, uma partição por padrão,
replicação 1, retenção default de 24 horas e armazenamento em volume. Redpanda Console v3.14.0
é ferramenta de inspeção. Não há autenticação/TLS no broker interno do laboratório.

O gerador escreve JSONL `payment_event_v1` e manifesto; ainda não publica Kafka. O contrato
canônico está em `src/pixguard_ai/domain/events.py`, com dados exclusivamente sintéticos,
UUID v5 e valores BRL decimais serializados como strings. O SDP/PySpark 4.3.0 lê texto de arquivo
offline; o serviço pipeline possui `networks: []`. O lock de dependências da aplicação não
representa cliente Kafka implementado. Dashboard e análise consomem resultados offline.

A PoC desta tarefa, em projeto Docker Compose isolado `pixguard-plan004`, usou ferramentas
CLI da imagem Kafka para criar tópico de teste, publicar duas mensagens artificiais e ler
chave, partição e offsets. Foram observados offsets 0 e 1, com end offset 2. A evidência é
pequena: essas mensagens não são eventos do contrato. Não comprova integração Python,
conector Spark, lote de 100.000 eventos ou exactly-once.

## Direção aceita e detalhes propostos

Introduzir primeiro transporte Kafka e captura contínua bruta com fechamento em snapshots
por faixas explícitas de offsets. Cada snapshot completo alimenta o pipeline offline existente;
Gold continua processamento de lote. Preservar o caminho JSONL atual como referência e fonte
de republicação. Rafael aceitou esse incremento e autorizou publicação incremental em 2026-11-12.
A aprovação da direção não aprova automaticamente os parâmetros detalhados abaixo.

1. Publicar fonte imutável concluída, checksum conferido, com chave `event_id` e envelope
   v1 inalterado. Headers de transporte identificam lote, checksum e ordinal. Criar explicitamente
   `pixguard.payment_event_v1`, inicialmente uma partição, RF=1, retenção de 24h e política delete.
2. Isolar cliente Kafka em adaptadores. Escolher versão do cliente Python após PoC na imagem
   app; `confluent-kafka` é candidato não testado nesta entrega. Produtor solicita idempotência
   e `acks=all`, registra recibos e trata ack incerto separadamente de sucesso/falha.
3. Capturar bytes e proveniência `(cluster, topic, partition, offset)` em segmentos duráveis.
   Publicar segmento e manifesto atomicamente, com fsync, antes do commit manual do próximo
   offset. Reentregas deduplicam pela coordenada. Retomada verifica hashes e limites disponíveis.
4. Exportar snapshot `[start,end)` imutável com JSONL e proveniência. Não UTF-8, tombstones,
   multiline e metadados de transporte inválidos vão à quarentena de transporte; JSON de uma
   linha inválido segue para a quarentena contratual existente. Nunca perder bytes silenciosamente.
5. Deduplicar logicamente sobre o snapshot completo e manter quarentena integral de grupos
   conflitantes, já aceita na ADR 0004. Não prometer deduplicação global entre snapshots distintos.
   Não descartar atrasados por watermark; métricas de atraso não alteram o contrato.
6. Publicar resultados somente quando a reconciliação fechar. Dashboard distingue produzido,
   confirmado, incerto, capturado, exportado, processado e quarentenado. Não lê segmentos parciais.

A garantia alvo é at-least-once com deduplicação, a validar. Idempotência do produtor não elimina
duplicatas de uma republicação em outra sessão. Não existe transação Kafka/filesystem ponta a ponta.
Replay usa grupo/capture ID novo e offsets explícitos; offset expirado gera falha, sem fallback
silencioso. Retenção e broker único não oferecem garantia de recuperação de produção.

## Alternativas

- **Spark diretamente no Kafka:** aproxima processamento contínuo, mas requer PoC de artefato
  conector JVM/Scala, rede e checkpoints; requer também política para retirar uma linha Silver
  anteriormente publicada quando um conflito chegar depois. Não foi testado nem rejeitado em definitivo.
- **Captura + snapshot offline:** permite reaproveitar regras de lote comprovadas e provar reconciliação
  por faixas; adiciona armazenamento intermediário e latência de fechamento. É a direção inicial aceita.
- **Somente CLI Kafka:** suficiente para smoke do broker; não atende manifestos, captura durável,
  validação canônica e recuperação exigidos para implementação produtiva.

## Consequências e segurança

O contrato v1 permanece inalterado, sem dados reais. O pipeline não se torna streaming só por receber
um arquivo originado no Kafka. Conflitos são tratados dentro da faixa fechada; publicação cumulativa
exigirá reconstrução integral ou mecanismo explícito de retração. Não somar Gold de snapshots
sobrepostos como se fossem disjuntos.

Novos artefatos ocupam disco e exigem política de retenção ainda não aprovada. Manter broker interno,
interfaces em loopback, logs sem payloads/segredos e IDs de arquivos validados. PLAINTEXT é limitação
local, não configuração de produção. Não alterar serviços/volumes existentes para executar PoCs.

## Critérios antes da implementação ser considerada entregue

- PoC do cliente Python fixado, key/headers/ack, consumo e restart em Docker Compose; se a alternativa
  Spark direta for escolhida, PoC do conector e checkpoint antes do código definitivo.
- Fixture com 15 mensagens: 10 válidos únicos, uma duplicata, duas linhas conflitantes de outro ID,
  um JSON inválido e um não UTF-8. Resultado: 10 válidos, 1 duplicata, 3 quarentenas contratuais,
  1 quarentena de transporte; todos os registros contabilizados.
- Interromper antes/depois de persistência e commit; retomar sem perda e sem coordenadas duplicadas.
  Verificar offset expirado, payload inválido, disco indisponível e broker indisponível.
- Lote limpo de **100.000** seed 42: origem, acks, captura, exportação e válidos iguais a 100.000;
  zero incertezas e perdas. Republicação no mesmo snapshot cumulativo: 200.000 físicos,
  100.000 válidos e 100.000 duplicatas. Conferir hashes e somas Decimal.
- Replay da mesma faixa em nova execução conserva resultados; regressão offline permanece válida.
  Registrar tempo, memória e disco medidos; não declarar SLA baseado em duas mensagens.

Esses testes são gates futuros, não resultados desta ADR. A implementação será decomposta na ordem:
PoC de cliente → infraestrutura/configuração → publicador → captura durável → snapshot/SDP → observabilidade
→ aceite integrado. Cada tarefa terá Issue própria criada pelo orquestrador, testes e PR revisada.

## Decisões pendentes e relações

Rafael aprovou o incremento inicial por snapshots e a publicação incremental em 2026-11-12.
Cliente/versionamento, retenção dos artefatos, thresholds e demais parâmetros operacionais continuam
pendentes. A versão do cliente será fixada depois de evidência em container. Esses detalhes propostos
não são apresentados como aprovados.

- [ADR 0003 — Dados sintéticos reproduzíveis](0003-dados-sinteticos-reproduziveis.md)
- [ADR 0004 — Execuções SDP por run_id e conflitos](0004-spark-sdp-local-runs.md)
- [Plano do projeto](../plan.md)
- Plano operacional local: `specs/architecture/phase-2-streaming.md` (ignorado pelo Git).
- Evidências locais: `specs/tasks/reports/PLAN-004/attempt-1.md`.
