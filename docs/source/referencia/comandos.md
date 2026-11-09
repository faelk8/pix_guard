# Comandos e configuração

Execute estes comandos a partir da raiz do repositório. O host chama o Docker Compose; os processos da
aplicação, Kafka, dashboard e documentação executam dentro de containers.

| Ação | Comando |
|---|---|
| Validar arquivos Compose | `docker compose config --quiet` |
| Subir Kafka, aplicação e painel | `docker compose up -d --wait dashboard` |
| Subir Kafka e Redpanda Console | `docker compose up -d --wait redpanda-console` |
| Gerar lote padrão de 100 mil | `docker compose run --rm --no-deps app pixguard generate` |
| Rodar os testes | `docker compose run --rm --no-deps app pytest` |
| Ver estado dos containers | `docker compose ps` |
| Consultar logs | `docker compose logs --tail=100 kafka app dashboard redpanda-console` |
| Encerrar containers, preservando volumes | `docker compose down` |

O comando `docker compose down -v` remove os volumes nomeados e os arquivos que eles contêm; não o use se
pretender preservar a geração local.

## Portas

| Serviço | Padrão | Configuração |
|---|---:|---|
| Dashboard Streamlit | 8501 | `PIXGUARD_DASHBOARD_PORT` |
| Redpanda Console | 8080 | `PIXGUARD_KAFKA_CONSOLE_PORT` |
| Kafka (rede interna) | 9092 | `PIXGUARD_KAFKA_BOOTSTRAP_SERVERS` |

Para ajustar as portas, copie `.env.example` para `.env` na raiz e altere os valores. `.env` é ignorado pelo
Git; não armazene credenciais reais nele.

## Testes no container

```bash
docker compose run --rm --no-deps app pytest
```

A suíte cobre contrato, geração determinística, armazenamento JSONL, estados do painel e encerramento dos
containers. Os testes e a aplicação usam Python 3.12 na imagem do projeto.
