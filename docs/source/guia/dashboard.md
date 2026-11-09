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
