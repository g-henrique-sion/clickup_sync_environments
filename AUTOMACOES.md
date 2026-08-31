# Funcionamento das automacoes ClickUp

Data de referencia desta documentacao: **28/08/2026**.

Este documento descreve o comportamento atualmente implementado no projeto
`clickup_sync_environments`. Em caso de divergencia com anotacoes antigas,
prevalecem `app/config/settings.py`, `app/automations/*`,
`app/services/*` e `app/core/clickup_client.py`.

Regra de seguranca operacional: nao criar, mover, apagar tasks, recriar
webhooks ou disparar automacoes em ambiente produtivo apenas para validar esta
documentacao. Validacoes em dados ativos devem comecar em modo somente leitura;
qualquer escrita em ClickUp exige autorizacao explicita.

## Visao geral

O projeto e um servico FastAPI que recebe webhooks do ClickUp, coloca os eventos
em uma fila duravel e executa automacoes de negocio a partir de eventos
`taskCreated` e `taskStatusUpdated`.

O ciclo normal e:

1. ClickUp envia webhook para `/webhook`.
2. `app/main.py` valida o segredo e normaliza o evento.
3. `app/services/webhook_queue.py` persiste o evento em `data/webhook_events.json`.
4. Workers processam os eventos em paralelo, com trava por task para evitar corrida.
5. `app/automations/engine.py` decide qual automacao deve rodar.
6. As automacoes executam operacoes no ClickUp usando `app/core/clickup_client.py`.

Pontos importantes:

- Tasks que ja estavam em um status de gatilho antes do webhook estar ativo nao
  disparam automaticamente; normalmente e necessario um novo evento de mudanca de
  status ou reprocessamento manual.
- Eventos antigos sao descartados quando a task ja esta claramente em outro
  status; se a task ainda estiver convergindo para o novo status, o evento e
  adiado e tentado novamente.
- A fila e persistente; se o processo reiniciar, eventos pendentes voltam a ser
  processados.

## Listas principais

| Papel | Variavel | ID atual | Observacao |
| --- | --- | --- | --- |
| Backoffice oficial | `SOURCE_LIST_ID` | `901327332419` | BACKOFFICE (SOMBRA). |
| Cadastros Pendentes | `SOURCE_RETURN_LIST_ID` | `901327332411` | Lista de retorno comercial. |
| Auditoria | `DEST_LIST_ID` | `901324383232` | Auditoria - Cooperados. |
| Ongoing | `ONGOING_SYNC_LIST_ID` | `901322296001` | Lista operacional pos-rateio. |
| Onboarding | `ONBOARDING_SYNC_LIST_ID` | `901323337719` | Lista de marco/onboarding comum. |
| Planejamento Black | `PLANEJAMENTO_BLACK_SYNC_LIST_ID` | `901321549851` | Lista de planejamento para planos Black. |
| Onboarding Black | `ONBOARDING_BLACK_SYNC_LIST_ID` | `901324946417` | Lista de marco/onboarding Black. |
| Adesao Reprovada | `ADESAO_REPROVADA_LIST_ID` | `901306778017` | Origem da criacao em demissoes. |
| Demissao/Exclusao | `DEMISSOES_LIST_ID` | `901325857066` | Destino de tasks reprovadas. |
| Inadimplentes | `INADIMPLENTES_LIST_ID` | `901326084050` | Lista da baixa de negativacao. |
| Candidatos | `TASK_NAME_FORMAT_RULES` | `901326902129` | Regra especifica de nome por campos. |

Os IDs acima sao os IDs oficiais esperados hoje. Em execucao, o valor efetivo
vem das variaveis de ambiente carregadas por `app/config/settings.py`.

## Fontes e escrita no ClickUp

Endpoint base usado pelo cliente: `https://api.clickup.com/api/v2`.

O projeto usa dois tokens:

| Token | Uso |
| --- | --- |
| `SOURCE_CLICKUP_TOKEN` | Leitura/escrita em listas de origem e fallback em algumas operacoes. |
| `DEST_CLICKUP_TOKEN` | Leitura/escrita em listas de destino e fallback em algumas operacoes. |

A funcao `fetch_task_any()` tenta recuperar a task com os dois tokens. Algumas
rotas usam preferencia pelo destino para tasks de relacionamento. Operacoes de
comentario, status, relacionamento, subtarefas e anexos tambem possuem fallback
entre tokens quando o helper correspondente permite.

## Eventos e ordem de processamento

### `taskCreated`

Quando chega `taskCreated`, o dispatcher executa, nessa ordem:

1. `onboarding_notify.run_task_created()`.
2. `task_name_on_create.run_task_created()`.
3. `adesao_reprovada_demissoes.run()`.

Se a task ainda nao estiver consistente na API do ClickUp, o evento e adiado
algumas vezes antes de ser descartado.

### `taskStatusUpdated`

Quando chega `taskStatusUpdated`, o dispatcher monta o contexto atual da task e
executa, nessa ordem:

1. `onboarding_notify.run_status_change()`.
2. `ativo_inicio_operacao.run()`.
3. `inadimplentes_finalizacao.run()`.
4. `planejamento_black_to_ongoing.run()`.
5. `relationship_unilateral_black.run()` para Planejamento Black/Onboarding Black.
6. `relationship_bilateral.run()` para Ongoing/Onboarding.
7. `auditoria_routing.run()` para Auditoria.
8. `environment_sync.run()` como fluxo final Backoffice/Auditoria/Cadastros.

A ordem importa. No status final `1a Fatura Com Desconto`, o dispatcher aplica
uma excecao para Planejamento Black: primeiro sincroniza a task relacionada em
Onboarding Black e somente depois move a task de Planejamento Black para Ongoing.
Isso evita perder o sync antes de a task sair da lista Planejamento Black.

## Automacoes de negocio

| ID | Automacao | Evento | Condicao principal | Acao |
| --- | --- | --- | --- | --- |
| A01 | Comentarios de onboarding | `taskCreated` | Task criada em lista configurada de onboarding | Comenta marcando os usuarios configurados. |
| A02 | Nome da task ao criar | `taskCreated` | Task criada em lista com regra em `TASK_NAME_FORMAT_RULES` | Renomeia a task usando campos configurados. |
| A03 | Adesao Reprovada -> Demissoes | `taskCreated` | Task criada em `ADESAO_REPROVADA_LIST_ID` | Cria marco em `DEMISSOES_LIST_ID`, copia dados e relaciona. |
| A04 | Comentarios de status em onboarding | `taskStatusUpdated` | Task em lista configurada de onboarding | Comenta a mudanca de status com mencao. |
| A05 | Inicio de Operacao | `taskStatusUpdated` | Status normalizado igual a `ativo` em lista configurada | Preenche campo de inicio de operacao se estiver vazio. |
| A06 | Inadimplentes - subtarefas | `taskStatusUpdated` | `NEGATIVADO` -> `A BAIXAR NEGATIVACAO` | Cria subtarefas obrigatorias sem duplicar. |
| A07 | Inadimplentes - trava reativa | `taskStatusUpdated` | `A BAIXAR NEGATIVACAO` -> `PAGO` | Valida subtarefas e comprovante; se faltar, volta o status e comenta. |
| A08 | Planejamento Black -> Ongoing | `taskStatusUpdated` | Planejamento Black em `1a Fatura Com Desconto` | Primeiro sincroniza Onboarding Black; depois move a propria task para Ongoing e aplica status `Ativo`. |
| A09 | Sync Planejamento Black <-> Onboarding Black | `taskStatusUpdated` | Status permitido/mapeado e task relacionada | Atualiza o status da task par. |
| A10 | Sync Ongoing <-> Onboarding | `taskStatusUpdated` | Status permitido e task relacionada | Atualiza o status da task par. |
| A11 | Auditoria -> Onboarding/Onboarding Black | `taskStatusUpdated` | Auditoria em `auditoria` | Cria marco na lista correta conforme plano. |
| A12 | Auditoria -> Ongoing/Planejamento Black | `taskStatusUpdated` | Auditoria em `enviado para rateio` | Move a task para a lista correta conforme plano. |
| A13 | Backoffice -> Auditoria | `taskStatusUpdated` | Backoffice em `cooperado aprovado` | Clona task para Auditoria. |
| A14 | Auditoria -> Cadastros Pendentes | `taskStatusUpdated` | Auditoria em `pend. comercial` | Clona para Cadastros Pendentes, copia dados e remove a task da Auditoria. |
| A15 | Cadastros Pendentes -> Auditoria | `taskStatusUpdated` | Cadastros Pendentes em `corrigido` | Clona de volta para Auditoria e preserva a origem. |

## Fluxos detalhados

### Backoffice -> Auditoria -> Cadastros Pendentes

Este fluxo fica em `app/automations/environment_sync.py`.

| Etapa | Origem | Gatilho | Destino | Comportamento |
| --- | --- | --- | --- | --- |
| Ida | `SOURCE_LIST_ID` | `SOURCE_TRIGGER_STATUS` (`cooperado aprovado`) | `DEST_LIST_ID` | Cria nova task na Auditoria com campos, anexos e comentarios. |
| Pendencia | `DEST_LIST_ID` | `DEST_RETURN_TRIGGER_STATUS` (`pend. comercial`) | `SOURCE_RETURN_LIST_ID` | Cria task em Cadastros Pendentes, copia dados e apaga a original da Auditoria. |
| Retorno | `SOURCE_RETURN_LIST_ID` | `SOURCE_RETURN_TRIGGER_STATUS` (`corrigido`) | `DEST_LIST_ID` | Cria nova task na Auditoria e preserva a task de Cadastros Pendentes. |

Na ida Backoffice -> Auditoria, os campos personalizados enviados sao limitados
ao schema da lista de destino. Ou seja: se um campo existe no Backoffice, mas nao
existe na Auditoria, ele nao deve ser criado ou copiado artificialmente na Auditoria.

### Auditoria -> Onboarding/Onboarding Black

Este fluxo fica em `app/automations/auditoria_routing.py`.

Quando uma task em `DEST_LIST_ID` entra no status `auditoria`, a automacao le o
campo `Plano de Adesao` (`0e009719-1e94-482a-825a-c359e268727e`) e decide o
destino:

| Plano | Destino |
| --- | --- |
| Valor presente em `AUDITORIA_ROUTING_BLACK_VALUES` | `AUDITORIA_ROUTING_ONBOARDING_BLACK_LIST_ID` |
| Qualquer outro valor | `AUDITORIA_ROUTING_ONBOARDING_LIST_ID` |

Valores Black padrao:

- `Black Linear 25%`.
- `BLACK`.
- `Performance 15% (COPEL/CELESC)`.
- `Max 25% (COPEL/CELESC)`.

A task criada e um marco, recebe relacionamento com a task de auditoria, recebe
status `telefone etapa 1` e recebe apenas campos que existem no schema da lista
de destino.

### Auditoria -> Ongoing/Planejamento Black

Quando uma task em Auditoria entra no status `enviado para rateio`, a propria
task e movida para:

| Plano | Destino |
| --- | --- |
| Plano Black | `AUDITORIA_RATEIO_BLACK_LIST_ID` / Planejamento Black |
| Plano nao Black | `AUDITORIA_RATEIO_ONGOING_LIST_ID` / Ongoing |

Durante o movimento, somente os campos que existem no schema da lista de destino
sao movidos. Campos ausentes no destino nao devem ser criados.

### Ongoing <-> Onboarding

Este fluxo fica em `app/automations/relationship_bilateral.py`.

Listas envolvidas:

- `ONGOING_SYNC_LIST_ID`.
- `ONBOARDING_SYNC_LIST_ID`.

Statuses sincronizados por padrao:

- `Aguardando Cadastro`.
- `Cadastro em Andamento`.
- `Ativo`.

A automacao procura a task par pelo relacionamento da task atual. Se encontrar a
task relacionada na lista esperada, aplica o mesmo status nela. Antes de atualizar,
revalida o status atual da origem para evitar aplicar evento antigo.

### Planejamento Black <-> Onboarding Black

Este fluxo fica em `app/automations/relationship_unilateral_black.py`.

Listas envolvidas:

- `PLANEJAMENTO_BLACK_SYNC_LIST_ID`.
- `ONBOARDING_BLACK_SYNC_LIST_ID`.

Mapeamento de status:

| Planejamento Black | Onboarding Black |
| --- | --- |
| `Troca Solicitada` | `Agendamento TT` |
| `Titularidade Alterada` | `Troca de TT` |
| `Cadastrado na Usina` | `Cadastro aprovado` |

Tambem existem statuses iguais permitidos via `BLACK_SYNC_ALLOWED_STATUSES`, por
padrao:

- `1a fatura sem inj`.
- `1a fatura com desconto`.

A automacao funciona nos dois sentidos para os pares mapeados e para statuses
iguais permitidos. Tambem usa cache de task par e revalidacao de status da origem.

### Planejamento Black -> Ongoing

Este fluxo fica em `app/automations/planejamento_black_to_ongoing.py`.

Quando uma task da lista Planejamento Black chega em
`PLANEJAMENTO_BLACK_TO_ONGOING_TRIGGER_STATUS` (`1a Fatura Com Desconto`), a
automacao sincroniza antes a task relacionada em Onboarding Black para o mesmo
status final. Depois move a propria task para
`PLANEJAMENTO_BLACK_TO_ONGOING_TARGET_LIST_ID` (Ongoing) e aplica
`PLANEJAMENTO_BLACK_TO_ONGOING_TARGET_STATUS` (`Ativo`).

### Inicio de Operacao

Este fluxo fica em `app/automations/ativo_inicio_operacao.py`.

Quando uma task em lista configurada entra em `ATIVO_INICIO_OPERACAO_TRIGGER_STATUS`
(`ativo`), o campo `ATIVO_INICIO_OPERACAO_FIELD_ID`
(`ebd051a1-d5b6-4cb1-861b-574a1f968663`) e preenchido com o primeiro dia do mes
da mudanca de status. Se o campo ja tiver valor ou nao existir na lista, a
automacao nao bloqueia a fila.

### Comentarios de onboarding

Este fluxo fica em `app/automations/onboarding_notify.py`.

A automacao comenta em tasks das listas configuradas em `ONBOARDING_NOTIFY_LIST_IDS`.
A configuracao atual marca somente `Christian Lopes de Moura`, via:

- `ONBOARDING_NOTIFY_USER_IDS`.
- `ONBOARDING_NOTIFY_USER_NAMES`.

Para evitar repeticao causada por retry/reentrega do webhook, a automacao busca
comentarios recentes e ignora mensagens iguais dentro da janela configurada por
`ONBOARDING_NOTIFY_DEDUP_LOOKBACK_SECONDS`.

### Adesao Reprovada -> Demissoes

Este fluxo fica em `app/automations/adesao_reprovada_demissoes.py`.

Quando uma task e criada em `ADESAO_REPROVADA_LIST_ID`, o sistema cria um marco
em `DEMISSOES_LIST_ID`, aplica `DEMISSOES_CREATE_STATUS`, copia campos
compativeis, copia anexos/comentarios e cria relacionamento entre as tasks.

### Nome automatico da task

Os formatadores ficam em:

- `app/automations/task_name_formatter.py`.
- `app/automations/task_name_on_create.py`.

Regra padrao:

- listas em `TASK_NAME_FORMAT_LIST_IDS` usam `TASK_NAME_TEMPLATE`, por padrao
  `{razao} - UC {uc}`;
- `razao` vem de `TASK_NAME_RAZAO_FIELD_ID`;
- `uc` vem de `TASK_NAME_UC_FIELD_ID`.

Regra especifica atual:

| Lista | Nome gerado | Campos |
| --- | --- | --- |
| `901326902129` | `{field_a} - {field_b}` | `6b668919-9c13-4127-bc2e-fa14eee95e8a` e `2ef3a097-4122-4c3d-9626-000087de9ced` |

Na pratica, para a lista de candidatos, o nome vira:

```text
Nome do Candidato - Cargo/Vaga
```

### Inadimplentes

Este fluxo fica em `app/automations/inadimplentes_finalizacao.py`.

A automacao faz duas coisas:

1. Quando a task sai de `NEGATIVADO` e entra em `A BAIXAR NEGATIVACAO`, cria as
   subtarefas obrigatorias que ainda nao existirem.
2. Quando a task sai de `A BAIXAR NEGATIVACAO` e entra em `PAGO`, valida as
   subtarefas e o comprovante. Se faltar algo, retorna para `A BAIXAR NEGATIVACAO`
   e comenta as pendencias.

Subtarefas padrao:

- `Solicitar a baixa da negativacao`.
- `Enviar comprovante de baixa ao cooperado`.

Comprovante padrao:

| Configuracao | Valor |
| --- | --- |
| `INADIMPLENTES_COMPROVANTE_FIELD_ID` | `3e4964bf-557b-4a47-8f86-b1bc43780910` |
| `INADIMPLENTES_COMPROVANTE_FIELD_NAME` | `Comprovante de Baixa` |

A trava reativa so vale para `A BAIXAR NEGATIVACAO` -> `PAGO`. Se alguem mover
de outro status diretamente para `PAGO`, essa automacao nao bloqueia.

## Regras de campos personalizados

| Regra | Comportamento |
| --- | --- |
| Campos diretos | `ENV_SYNC_USE_DIRECT_FIELDS=1` permite montar payload direto por campo da task. |
| Campos nao gravaveis | Formula, rollup, progress, automatic_progress e button sao ignorados. |
| Campos vazios | Campos sem valor util nao entram no payload. |
| Campos de anexo | Nao entram no JSON de `custom_fields`; sao tratados pelo fluxo de upload de anexos. |
| Schema do destino | Fluxos configurados para respeitar destino enviam somente campos existentes na lista alvo. |
| Campos ausentes | Campo ausente no destino nao deve ser criado automaticamente pela automacao. |

O filtro de existencia no destino e aplicado nos fluxos que partem de Auditoria
para Onboarding, Onboarding Black, Ongoing e Planejamento Black. Tambem e aplicado
na ida Backoffice -> Auditoria.

## Regras de anexos e comentarios

Os anexos sao tratados em `app/core/clickup_client.py`.

Regra principal:

- se o arquivo esta em um campo personalizado de anexo, ele deve ser enviado para
  o campo correspondente no destino;
- se o arquivo existe apenas como anexo da task, ele deve ser enviado apenas como
  anexo da task;
- se o ClickUp tambem expuser o anexo de campo como anexo da task, o codigo usa
  chaves de identidade para nao enviar o mesmo arquivo duas vezes;
- nao existe mais inferencia por nome de arquivo para tentar jogar anexo de task
  dentro de campo personalizado;
- quando o campo de anexo nao existe no schema de destino, o arquivo e preservado
  como anexo da task, sem criar campo novo.

Protecoes de arquivo:

- downloads de PDF sao validados para garantir que o conteudo comeca como PDF;
- respostas HTML baixadas como arquivo sao rejeitadas;
- uploads de campo personalizado que retornam arquivo vazio geram retry/erro.

Comentarios:

- comentarios e replies sao clonados em ordem cronologica;
- comentarios de anexo podem ser reescritos para apontar para o anexo copiado;
- o formatador evita acumular assinatura/data repetida quando o comentario ja
  foi formatado em copia anterior.

## Webhook, fila e recuperacao

| Componente | Arquivo | Papel |
| --- | --- | --- |
| Endpoint HTTP | `app/main.py` | Recebe `/webhook` e expoe `/health`. |
| Fila duravel | `app/services/webhook_queue.py` | Persiste, deduplica e processa eventos. |
| Guard de webhook | `app/services/webhook_guard.py` | Mantem webhook ativo, remove duplicatas e recria quando configurado. |
| Gerenciador manual | `manage_webhook.py` | Utilitario para listar/criar/remover webhooks. |

Caracteristicas da fila:

- arquivo padrao: `data/webhook_events.json`;
- workers paralelos configurados por `WEBHOOK_WORKERS`;
- trava por `task_id` para evitar duas automacoes simultaneas na mesma task;
- dedup de eventos iguais;
- coalescencia de `taskStatusUpdated` para manter o evento mais recente por task;
- retry exponencial para falhas temporarias;
- descarte de falhas nao retryable, como 400, 401, 403, 404 e 422.

Caracteristicas do guard:

- usa `WEBHOOK_ENDPOINT` e `WEBHOOK_TEAM_IDS`;
- confere eventos esperados em `WEBHOOK_EXPECTED_EVENTS`;
- pode criar webhook ausente, deletar duplicatas e recriar webhook com falhas;
- pode persistir segredo no `.env` quando `WEBHOOK_GUARD_PERSIST_SECRETS=1`.

## Variaveis de ambiente importantes

### Integracao ClickUp

| Variavel | Funcao |
| --- | --- |
| `SOURCE_CLICKUP_TOKEN` | Token principal para listas de origem. |
| `DEST_CLICKUP_TOKEN` | Token principal para listas de destino. |
| `DEST_WORKSPACE_ID` | Workspace usado em endpoints v3, principalmente anexos de custom field. |

### Listas e gatilhos

| Variavel | Padrao/uso |
| --- | --- |
| `SOURCE_LIST_ID` | Lista Backoffice monitorada. |
| `SOURCE_TRIGGER_STATUS` | `cooperado aprovado`. |
| `DEST_LIST_ID` | Lista Auditoria. |
| `DEST_RETURN_TRIGGER_STATUS` | `pend. comercial`. |
| `SOURCE_RETURN_LIST_ID` | Lista Cadastros Pendentes. |
| `SOURCE_RETURN_TRIGGER_STATUS` | `corrigido`. |
| `ONGOING_SYNC_LIST_ID` | Lista Ongoing. |
| `ONBOARDING_SYNC_LIST_ID` | Lista Onboarding. |
| `PLANEJAMENTO_BLACK_SYNC_LIST_ID` | Lista Planejamento Black. |
| `ONBOARDING_BLACK_SYNC_LIST_ID` | Lista Onboarding Black. |
| `ADESAO_REPROVADA_LIST_ID` | Lista Adesao Reprovada. |
| `DEMISSOES_LIST_ID` | Lista Demissao/Exclusao. |
| `INADIMPLENTES_LIST_ID` | Lista Inadimplentes. |

### Regras de status e roteamento

| Variavel | Uso |
| --- | --- |
| `DEST_SYNC_ALLOWED_STATUSES` | Status sincronizados entre Ongoing e Onboarding. |
| `BLACK_SYNC_ALLOWED_STATUSES` | Status iguais sincronizados entre Black e Onboarding Black. |
| `BLACK_SYNC_STATUS_MAP` | JSON com pares Planejamento Black -> Onboarding Black. |
| `AUDITORIA_ROUTING_TRIGGER_STATUS` | Status que cria marco de onboarding. |
| `AUDITORIA_RATEIO_TRIGGER_STATUS` | Status que move para Ongoing/Planejamento Black. |
| `AUDITORIA_ROUTING_BLACK_VALUES` | Valores de plano considerados Black. |
| `AUDITORIA_ROUTING_ONBOARDING_NEW_TASK_STATUS` | Status inicial do marco criado. |
| `PLANEJAMENTO_BLACK_TO_ONGOING_TRIGGER_STATUS` | Status final que move Planejamento Black para Ongoing. |
| `PLANEJAMENTO_BLACK_TO_ONGOING_TARGET_STATUS` | Status aplicado no Ongoing. |

### Campos e nomes

| Variavel | Uso |
| --- | --- |
| `CLONE_FIELD_MAP` | Mapa legado/de apoio de campos origem -> destino. |
| `ENV_SYNC_USE_DIRECT_FIELDS` | Habilita copia direta de campos quando possivel. |
| `ATIVO_INICIO_OPERACAO_FIELD_ID` | Campo preenchido ao entrar em Ativo. |
| `TASK_NAME_FORMAT_LIST_IDS` | Listas com formatacao padrao de nome. |
| `TASK_NAME_RAZAO_FIELD_ID` | Campo usado como razao social. |
| `TASK_NAME_UC_FIELD_ID` | Campo usado como UC. |
| `TASK_NAME_TEMPLATE` | Template padrao de nome. |
| `TASK_NAME_FORMAT_RULES` | JSON com regras especificas por lista. |

### Webhook e HTTP

| Variavel | Uso |
| --- | --- |
| `WEBHOOK_ENDPOINT` | URL publica do endpoint `/webhook`. |
| `WEBHOOK_SECRET` / `WEBHOOK_SECRETS` | Segredos aceitos pelo endpoint. |
| `WEBHOOK_TEAM_IDS` | Workspaces/teams monitorados pelo guard. |
| `WEBHOOK_EXPECTED_EVENTS` | Eventos esperados no webhook. |
| `WEBHOOK_WORKERS` | Quantidade de workers da fila. |
| `WEBHOOK_QUEUE_MAXSIZE` | Tamanho maximo da fila em memoria. |
| `WEBHOOK_GUARD_*` | Flags de criacao, remocao de duplicatas e recriacao de webhook. |
| `CLICKUP_HTTP_*` | Retry, backoff, timeout e pool HTTP. |

## Diagnostico rapido

### Automacao nao disparou

Verifique:

- se o webhook esta ativo no ClickUp e aponta para `WEBHOOK_ENDPOINT` correto;
- se `/health` mostra fila, workers e guard funcionando;
- se a lista da task corresponde a alguma variavel configurada;
- se o status do evento normalizado corresponde ao gatilho;
- se a task ja estava no status antes de o webhook ficar ativo;
- se o evento foi descartado como stale porque a task ja estava em outro status.

### Task nao foi clonada ou movida

Verifique:

- `SOURCE_LIST_ID`, `DEST_LIST_ID`, `SOURCE_RETURN_LIST_ID` e status de gatilho;
- se `Plano de Adesao` esta preenchido para rotas de Auditoria;
- se a lista destino existe e aceita o status alvo;
- logs `env_sync.*`, `auditoria_routing.*` ou `planejamento_black_to_ongoing.*`.

### Campo personalizado nao copiou

Verifique:

- se o campo existe no schema da lista destino;
- se o tipo do campo e gravavel;
- se o valor na origem esta vazio;
- se o campo e de anexo, pois ele nao entra no payload JSON comum;
- logs `schema_destino.aplicado` para ver quantos campos foram descartados.

### Anexo duplicou ou nao abriu

Verifique:

- se o arquivo estava em campo personalizado ou apenas como anexo da task;
- se o mesmo arquivo apareceu como anexo de campo e anexo da task, caso em que a
  deduplicacao deve impedir envio duplo;
- se o download retornou HTML ou PDF invalido;
- se o campo de anexo existe no destino;
- logs `clone_attachments`, `downloaded_attachment_not_pdf` e
  `downloaded_attachment_is_html`.

### Comentario repetiu em onboarding

Verifique:

- se `ONBOARDING_NOTIFY_DEDUP_LOOKBACK_SECONDS` esta ativo;
- se a mensagem e exatamente igual apos normalizacao;
- se houve retry/reentrega de webhook;
- se a task recebeu comentarios por copia de outro fluxo, que e diferente da
  notificacao de onboarding.

### Sync de relacionamento nao funcionou

Verifique:

- se a task tem relacionamento com a task par;
- se a task par esta na lista esperada;
- se o status esta em `DEST_SYNC_ALLOWED_STATUSES`, `BLACK_SYNC_ALLOWED_STATUSES`
  ou `BLACK_SYNC_STATUS_MAP`;
- se o evento foi ignorado como `stale_runtime`.

### Trava de Inadimplentes nao bloqueou

Verifique:

- se o movimento foi exatamente `A BAIXAR NEGATIVACAO` -> `PAGO`;
- movimentos vindos de outros status para `PAGO` nao sao bloqueados;
- se as duas subtarefas existem e estao concluidas;
- se o comprovante esta no campo configurado ou, quando permitido, em anexo da task.

## Arquivos de codigo mais relevantes

| Arquivo | Responsabilidade |
| --- | --- |
| `app/main.py` | FastAPI, webhook e healthcheck. |
| `app/models/schemas.py` | Modelos de payload de webhook. |
| `app/services/webhook_queue.py` | Fila duravel, retry e workers. |
| `app/services/webhook_guard.py` | Guard que mantem webhook ativo. |
| `app/automations/engine.py` | Dispatcher das automacoes. |
| `app/automations/common.py` | Contexto comum, normalizacao e helpers. |
| `app/automations/environment_sync.py` | Backoffice/Auditoria/Cadastros Pendentes. |
| `app/automations/auditoria_routing.py` | Auditoria -> Onboarding/Ongoing/Planejamento Black. |
| `app/automations/relationship_bilateral.py` | Sync Ongoing <-> Onboarding. |
| `app/automations/relationship_unilateral_black.py` | Sync Planejamento Black <-> Onboarding Black. |
| `app/automations/planejamento_black_to_ongoing.py` | Movimento Planejamento Black -> Ongoing. |
| `app/automations/onboarding_notify.py` | Comentarios de onboarding. |
| `app/automations/ativo_inicio_operacao.py` | Preenchimento de Inicio de Operacao. |
| `app/automations/inadimplentes_finalizacao.py` | Subtarefas e trava reativa de Inadimplentes. |
| `app/automations/adesao_reprovada_demissoes.py` | Adesao Reprovada -> Demissoes. |
| `app/automations/task_name_formatter.py` | Formatacao reutilizavel de nome. |
| `app/automations/task_name_on_create.py` | Renomear ao criar task. |
| `app/core/clickup_client.py` | Cliente ClickUp, campos, anexos, comentarios e fallback de token. |
| `app/config/settings.py` | Configuracao por variaveis de ambiente. |
| `manage_webhook.py` | Utilitario manual de webhook. |
| `start_local.py` | Inicializacao local com ngrok e uvicorn. |
