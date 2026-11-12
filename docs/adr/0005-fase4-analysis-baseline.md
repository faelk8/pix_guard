# ADR 0005 — Baseline explicável e avaliação sintética da FASE 4

- Status: **proposed** — aguardando decisão de Rafael; nenhuma regra/threshold aprovada nesta ADR.
- Data: 2026-11-10.
- Issue: #15 (planejamento fundamental PLAN-003).
- Responsável: arquitetura de dados.

## Contexto observado

PixGuard é um laboratório com eventos fictícios, não um sistema que decide pagamentos. O gerador atual em
`src/pixguard_ai/infrastructure/synthetic/generator.py` sorteia `random.randrange(100) >= 95` para escolher
`high_amount`: chance nominal de 5%, sem exigir frequência exata em cada lote. Valores desse cenário ficam
entre R$10.000,00 e R$50.000,00; os normais, entre R$1,00 e R$2.000,00, com extremos inclusivos. O cenário
origina o rótulo `ground_truth.is_suspicious`. A separação por valor foi construída artificialmente.

O contrato `payment_event_v1` separa `features` e `ground_truth`. A Silver da FASE 3 preserva ambos, bem como
payload bruto e proveniência, e acrescenta `amount_decimal` em Decimal(12,2). Logo, ler Silver inteira como
feature causaria vazamento do alvo. Gold já agrega eventos e não serve como unidade de avaliação individual.

As Issues #10/#11 tratam do processamento offline e do dashboard existentes localmente; #14 registra a
ADR 0004. A FASE 3 lê JSONL, não Kafka. Duplicatas equivalentes colapsam e grupos com mesmo event_id e
payload divergente vão integralmente à quarentena, conforme decisão já confirmada por Rafael. A avaliação
nova consome o Silver resultante sem mudar essa política. Os resultados anteriores de 100.000 eventos são
evidências da FASE 3, não métricas de classificação da FASE 4.

## Proposta de decisão

### Baseline fixa, sem treinamento

Propõe-se `amount_threshold_v1`, versão `1.0.0`: sinalizar quando `amount_decimal >= Decimal("10000.00")`.
O score é o valor em centavos inteiros, sem significado probabilístico. Emitir
`AMOUNT_AT_OR_ABOVE_THRESHOLD` ou `AMOUNT_BELOW_THRESHOLD`, valor, limite, operador e versão. Valores
inválidos falham a execução, sem virar previsão negativa. O único atributo permitido é `amount_decimal`;
`event_id` serve apenas de chave e nunca influencia a decisão.

Projetar explicitamente a entrada permitida antes de chamar o scorer. Proibir rótulos, `scenario_id`,
payload bruto/canônico, identidade, status, timestamps e demais metadados como inputs preditivos. Configuração
com outra feature deve ser rejeitada. Persistir predições sem alvo e só depois juntar referência por
`event_id`, exigindo unicidade, cobertura integral e relação 1:1. Testes devem provar invariância da
pontuação quando os rótulos de avaliação forem trocados ou removidos.

O limite é conhecido pela inspeção do gerador, não escolhido por otimização ou política bancária. A
separação perfeita é esperada por construção; só será reportada como resultado depois de medida.

### Protocolo congelado

Propõe-se `fixed_simulator_conformance_v1`: avaliar todos os Silver elegíveis, sem treino, ajuste ou holdout.
O lote padrão tem seed 42 e 100.000 eventos, porém não constitui teste cego. Trata-se de conferir uma regra
contra o comportamento conhecido do simulador. O relatório deve declarar essa finalidade explicitamente,
sem alegar generalização para fraude bancária.

Congelar config canônica e hash antes da avaliação: allowlist, limite/operador, definição de score, classe
positiva, métricas, políticas de erro/indefinição, versões e população. Fixar source_run_id concluído,
cópia/hash do manifesto de origem e inventário/hash dos Parquet Silver. Registrar seed somente se houver
proveniência; o manifesto atual não a garante. Config alterada exige nova versão/run.

Ajuste de threshold ou ML exige nova decisão: hipótese, objetivo/custo de erros, separação de
train/validation/test respeitando tempo e grupos relevantes, ajuste só na validação e avaliação reservada.
Não reutilizar resultados de teste para escolher a regra final. Outro seed não elimina o viés artificial.

### Métricas propostas

Avaliar por evento único, com positivo definido exclusivamente pelo rótulo sintético. Publicar TP, FP, FN,
TN, suportes positivos/negativos, prevalência, alertas, precision, recall, F1, FPR e accuracy secundária,
acompanhada da baseline “sempre negativo”. `precision=TP/(TP+FP)`, `recall=TP/(TP+FN)` e
`F1=2TP/(2TP+FP+FN)`. Denominador zero retorna `null`, motivo e `defined=false`; nunca NaN nem zero inventado.
F1 pode ser zero quando há positivos e nenhum alerta, embora precision fique indefinida.

Calcular PR-AUC no score em centavos, com `BinaryClassificationEvaluator`, `areaUnderPR`, `numBins=0`,
sem pesos e versão Spark registrada. Não alimentar o evaluator com a decisão binária, nem confundir
PR-AUC com average precision. Exigir ambas as classes por política local; em conjunto vazio/monoclasse,
PR-AUC é indefinida com aviso. Vazio não representa sucesso de desempenho. O guia oficial descreve as
[métricas de classificação e limitações de accuracy](https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html);
a [API oficial do evaluator](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.evaluation.BinaryClassificationEvaluator.html)
documenta `areaUnderPR` e `numBins`.

### Execução e consumo

Reutilizar Docker Compose, imagem Spark/PySpark do pipeline e volume local. Agregações rodam em PySpark;
dashboard permanece sem Spark. Código, testes e interface executam em containers. Validar primeiro fixtures
com todos os tipos de erro, fronteiras monetárias, leakage e denominadores nulos; depois lote padrão de
100.000 e reexecução comparada em novo container. Conferir `N=Silver=predictions=evaluation=TP+FP+FN+TN`,
sem exclusão silenciosa, registrando também duplicatas/quarentena da origem.

Gravar outputs imutáveis em `analysis-runs/<analysis_run_id>/`: protocolo, cópia/inventário da origem,
predições e avaliação separados em Parquet, métricas, resumo limitado a 20 exemplos e manifesto atômico
publicado por último. Incluir hashes, versões de código/ambiente, source_run_id, timestamps UTC, contagens e
reconciliação. Run parcial/falho/corrompido não pode ser consumido como concluído; não modificar FASE 3.

O painel mostra regra/limite, fonte e run, matriz de confusão, métricas e explicações de valores indefinidos,
com banner sintético e aviso de que a separação artificial não comprova detecção de fraude. Sem ajuste
interativo sobre a avaliação congelada. Retenção, Kafka, dados reais e bloqueio de pagamentos ficam fora.

## Alternativas consideradas

1. **Treinar ML imediatamente:** acrescentaria complexidade para reaprender a separação por valor já conhecida.
   Recomenda-se adiar; nova hipótese/dados e comparação justificada seriam pré-requisitos, não garantia de utilidade.
2. **Ajustar threshold no próprio lote avaliado:** selecionaria a regra usando o resultado que pretende avaliar.
   Rejeitado na proposta; qualquer ajuste futuro exige protocolo separado de avaliação.
3. **Criar holdout aleatório da distribuição atual:** não remove a informação conhecida das faixas nem cria
   evidência de generalização. Desnecessário para a conformidade declarada desta baseline fixa.
4. **Avaliar apenas accuracy ou grupos Gold:** esconde classe rara ou muda a unidade de análise. Preferir
   contagens por evento, precision/recall/F1 e PR-AUC, explicando a artificialidade.

## Consequências e riscos

A proposta é reproduzível e explicável, com baixo custo operacional e sem novo banco/serviço. O custo é
manter contratos/artefatos e testes de isolamento, além de medir recursos do lote. Snapshots e versões
permitem auditoria, mas o volume continua local e sem retenção automática/SLA.

Resultados possivelmente perfeitos só demonstram coerência com o simulador. Percentuais nominais,
características sintéticas e ausência de custos reais não autorizam comparação com antifraude bancário.
Ground truth presente em Silver exige barreiras testadas; comentários ou blacklist não são suficientes.
O plano original não executou código. Implementações subsequentes nas Issues #16–#18 mediram conformidade e
adicionaram o leitor visual; os resultados e a validação local não convertem esta proposta em decisão aprovada.

## Decisões pendentes e execução posterior

Rafael ainda precisa decidir formalmente se aceita a regra e limite inclusivo, protocolo de conformidade sem
holdout, política de métricas indefinidas/PR-AUC, artefatos/painel e adiamento de ML. O status permanece
**proposed**. A implementação local fornece evidência para revisão, não aprovação de produto.

## Implementação e evidência local (Issues #16–#18)

`RULE-001` (#16) implementou a baseline fixa em centavos. `ANALYSIS-001` (#17) avaliou a Silver de
`batch-100k-001` (seed 42, 100.000 eventos), persistiu predições sem rótulo antes do join, publicou métricas e
comparou runs imutáveis. `UI-003` (#18) acrescentou uma tela que valida os hashes dos artefatos e mostra matriz,
métricas, baseline sempre-negativo e até vinte exemplos.

No conjunto medido houve 5.008 positivos e 94.992 negativos: TP=5.008, FP=0, FN=0 e TN=94.992. Precision,
recall, F1 e PR-AUC foram 1,0; a accuracy da baseline sempre-negativo foi 0,94992. Dois runs (`eval-003` e
`eval-004`) foram logicamente equivalentes em container novo. As faixas do gerador são disjuntas: positivos
R$10.000–R$50.000 e demais R$1–R$2.000. Portanto, a perfeição é esperada por construção e não mede fraude real.

**Limite de proveniência:** o manifesto FASE 3 não tinha checksum histórico dos Parquet Silver. A análise
inventariou os arquivos no início e confirmou o inventário e manifesto novamente ao final, detectando mudança
durante o processamento. Isso não prova que a Silver não foi alterada antes da análise.

A Issue #19 documenta comportamento e resultados. A Issue #20 prepara um artigo local. A PR/revisão e qualquer
publicação GitHub dependem de autorização de Rafael; `specs/` permanece local e ignorada.
