# Documentacao de Automacoes - clickup_sync_environments

Atualizado em: 2026-07-28

## Automacoes ativas

1. `environment_sync` (`app/automations/environment_sync.py`)
   - Clone de task entre ambientes a partir dos gatilhos configurados.
   - Suporta ida e retorno com clone de campos, anexos e comentarios.
   - Gatilho de retorno atual (`DEST_RETURN_TRIGGER_STATUS`): `pend. comercial`.

2. `relationship_bilateral` (`app/automations/relationship_bilateral.py`)
   - Sincronizacao bilateral de status entre `Ongoing` e `Onboarding`.
   - Usa relacionamento entre tasks para localizar task par.

3. `relationship_unilateral_black` (`app/automations/relationship_unilateral_black.py`)
   - Sincronizacao de status entre `Planejamento Black` e `Onboarding Black`.
   - Opera por relacionamento entre tasks e cache de task par.
   - Fluxo atual:
     - Bilateral para statuses mapeados (quando os nomes diferem entre listas).
     - Direto para statuses iguais presentes em `BLACK_SYNC_ALLOWED_STATUSES`.

   - Mapeamento de status (equivalencia):
     - `Troca Solicitada` <-> `Agendamento TT`
     - `Titularidade Alterada` <-> `Troca de TT`
     - `Cadastrado na Usina` <-> `Cadastro aprovado`

4. `auditoria_routing` (`app/automations/auditoria_routing.py`)
   - Ao entrar em `AUDITORIA`, cria marco em `Onboarding` ou `Onboarding Black` conforme `Plano de adesao`.
   - Ao entrar em `ENVIADO PARA RATEIO`, move task de auditoria para `Ongoing` ou `Planejamento Black` conforme plano.

5. `ativo_inicio_operacao` (`app/automations/ativo_inicio_operacao.py`)
   - Ao entrar em `Ativo`, preenche `Inicio da Operacao` com o dia 1 do mes da mudanca de status (se vazio).

6. `onboarding_notify` (`app/automations/onboarding_notify.py`)
   - Publica comentarios de criacao e mudanca de status nas listas de onboarding com mencao dos usuarios configurados.
   - Configuracao atual: mencao somente para `Christian Lopes de Moura`.

7. `adesao_reprovada_demissoes` (`app/automations/adesao_reprovada_demissoes.py`)
   - Ao criar task em `Adesao Reprovada`, cria marco em `Demissoes` com relacionamento e copia de dados.

8. `task_name_on_create` (`app/automations/task_name_on_create.py`)
   - Ao criar task em listas com regra especifica em `TASK_NAME_FORMAT_RULES`, atualiza o nome da propria task.
   - Uso atual: lista `901326902129` (`Candidatos`) com `Nome do Candidato - Cargo/Vaga`.

9. `inadimplentes_finalizacao` (`app/automations/inadimplentes_finalizacao.py`)
   - Lista: `901326084050` (`Inadimplentes`).
   - Ao mudar de `NEGATIVADO` para `A BAIXAR NEGATIVACAO`, cria subtarefas obrigatorias de finalizacao sem duplicar:
     - `Solicitar a baixa da negativacao`
     - `Enviar comprovante de baixa ao cooperado`
   - Ao tentar mover para `PAGO`, valida:
     - as duas subtarefas existem;
     - as duas subtarefas estao concluidas;
     - o comprovante esta anexado no campo configurado ou, se nao houver campo configurado, em anexos da task.
   - Se faltar algum item, retorna a task para `A BAIXAR NEGATIVACAO` e comenta as pendencias.

## Regras de nome de task

- Regra padrao: listas configuradas em `TASK_NAME_FORMAT_LIST_IDS` continuam usando `Razao Social - UC`.
- Regra especifica por lista:
  - Lista `901326902129`: nome da task = `Nome do Candidato - Cargo/Vaga`.
  - Campos usados nessa regra:
    - `Nome do Candidato`: `6b668919-9c13-4127-bc2e-fa14eee95e8a`
    - `Cargo/Vaga`: `2ef3a097-4122-4c3d-9626-000087de9ced`
