# Como ler um evento sintético

Um evento representa um pagamento fictício. Ele tem informações de contexto, atributos que poderiam ser usados
em análises e uma seção separada com a resposta esperada do simulador, chamada `ground_truth`.

```json
{
  "schema_version": "payment_event_v1",
  "event_id": "55033c7a-506a-5642-ba85-07da5f32c230",
  "event_time": "2025-01-01T00:00:00Z",
  "ingested_at": "2025-01-01T00:00:00.010000Z",
  "label_is_synthetic": true,
  "features": {
    "account_id_hash": "260175160abe09d4984e8cdc918e7fe6c33cddda5ad4dc99f79fef2b1527625a",
    "device_id_hash": "fdab55499bb551f59b006bb3b75b750a02c8bd2e9eaeb11149c3199008c83830",
    "merchant_id": "merchant_0380",
    "amount_brl": "66.56",
    "location_region": "region_03",
    "channel": "mobile",
    "status": "completed"
  },
  "ground_truth": {
    "scenario_id": "normal",
    "is_suspicious": false
  }
}
```

`event_time` indica quando o evento aconteceu na simulação; `ingested_at` registra quando foi escrito pelo
gerador. `Z` significa que o horário está em UTC, um padrão comum para comparar horários entre sistemas.

`features` contém os campos que um experimento poderia analisar. `ground_truth` guarda a classificação criada
pelo simulador e deve permanecer separada das features para não entregar a resposta ao modelo. `label_is_synthetic`
avisa que tanto o pagamento quanto o rótulo são artificiais.

Os identificadores têm o prefixo `synth`; CPF, nome, número de conta ou chave Pix de pessoas reais não fazem
parte do contrato. A versão implementada valida campos e valores aceitos e rejeita campos inesperados.

## Gerar o lote

Na raiz do repositório, execute:

```bash
docker compose run --rm --no-deps app pixguard generate --seed 42 --count 100000 --output /data/pixguard/events.jsonl
```

O gerador produz uma linha JSON por evento, escreve incrementalmente e mantém um manifesto de progresso. Repetir
a mesma seed e configuração recria os mesmos bytes e IDs. Os valores de `ground_truth` descrevem somente as
regras do simulador; não são evidência de fraude real.
