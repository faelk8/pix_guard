# Acompanhar a geração no painel

O painel usa Streamlit, uma ferramenta Python que transforma um script em uma página web. Nesta fase, a página
mostra o estado do gerador e não faz análise de fraude.

Inicie o serviço pelo Docker Compose:

```bash
docker compose up -d --wait dashboard
```

Abra http://localhost:8501. Um banner identifica os dados como sintéticos. Durante ou depois da geração, confira:

* estado (em execução, concluído, falhou ou interrompido);
* eventos produzidos e solicitados;
* barra de progresso;
* tamanho gravado e horários da execução;
* erro registrado, se houver.

O painel lê o manifesto persistido no volume compartilhado. Um número ausente não é apresentado como zero, e
uma geração incompleta não substitui silenciosamente um lote anterior. O painel atualiza a leitura a cada dois
segundos.

Para confirmar um lote concluído, gere os 100 mil eventos com o comando da página
:doc:`contrato` e confira `status: completed`, `count_written: 100000` e `progress: 1.0` no painel.

## Ver resultados da FASE 3

Depois de gerar o JSONL, rode `dry-run` e depois `process` conforme o
:doc:`processamento-fase-3`. O dashboard inclui a seção **Processamento offline — FASE 3**. Ela escolhe o
manifesto concluído válido com o horário `finished_at` UTC mais recente e mostra:

* `run_id` e horários de início/fim;
* contagens de entrada/Bronze, Silver, duplicatas removidas, quarentena e grupos Gold;
* uma tabela e um gráfico feitos com até 20 grupos Gold resumidos pelo pipeline;
* aviso quando uma execução posterior falhou, preservando as medidas do último sucesso.

O app leve lê apenas o manifesto e o resumo JSON limitado. O Spark já escreveu os Parquet no volume e o app não
inicia uma sessão Spark durante a renderização. A validação confrontou o resumo com a saída Gold real. BRL fica
como decimal textual exato; o gráfico representa apenas contagem de eventos. O aviso de dado sintético também
explica que os rótulos e agregados não medem desempenho antifraude real.

Se ainda não houver execução concluída válida, a seção informa esse estado e não inventa zeros. Consulte o
manifesto e o guia para distinguir artefato ausente, execução falha ou resultado concluído.

## Acompanhar o transporte Kafka — FASE 2

A seção **Transporte Kafka — FASE 2** relê os manifestos a cada dois segundos e mostra publicação, captura e
snapshot exportado como etapas independentes. Os estados `validando` e `publicando` indicam trabalho em curso;
`capturando` indica que a faixa Kafka ainda pode crescer; `concluído`, `interrompido` e `falhou` vêm do manifesto.
Um manifesto ausente aparece como indisponível, e uma entrada inválida produz uma mensagem genérica sem expor
o texto bruto do erro.

As contagens de eventos produzidos, confirmados, incertos e não enviados pertencem à publicação. A captura
mostra a contagem disponível no manifesto e os limites da faixa física. Enquanto está ativa, o lag é calculado
como `fim exclusivo - próximo offset durável`; são posições físicas, não quantidade de eventos de negócio nem
latência. A ausência de uma posição confirmada é mostrada como indisponível, não como zero.

Um snapshot concluído permanece associado à captura que o originou, mesmo quando uma captura posterior está
ativa. O painel mostra separadamente registros físicos, exportados, duplicados e em quarentena de transporte
ou de contrato. Ele lê apenas manifestos validados: não abre segmentos Kafka nem exibe payloads. Os eventos
apresentados são sintéticos e não medem desempenho antifraude real.
