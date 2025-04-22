# Extract Facebook Ads

## Sobre
Projeto para extração de dados de campanhas do Facebook Ads. O script obtém informações das campanhas ativas via API do Facebook e armazena os resultados em um banco de dados PostgreSQL.

## Execução
O projeto pode ser executado localmente na sua máquina ou no AWS Glue

### Local
```bash
poetry run python facebook_local.py
```
## AWS Glue
Upload do script facebook_glue.py para AWS Glue e configuração dos parâmetros necessários no job.

### Configuração
Crie um arquivo .env baseado no .env_exemple com suas credenciais:

```bash
API_URL="https://graph.facebook.com/v19.0"
ACCOUNT_ID="act_seu_account_id"
TOKEN="seu_token"
PG_URL="jdbc:postgresql://seu_host:5432/sua_database"
PG_TABLE="sua_tabela"
PG_USER="seu_usuario"
PG_PASSWORD="sua_senha"
```
