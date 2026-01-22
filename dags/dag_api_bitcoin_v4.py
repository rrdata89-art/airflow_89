"""
DAG: Pipeline Bitcoin V4
Descrição: Orquestração completa do ETL de Bitcoin
Arquitetura: RAW → TRUSTED → REFINED (em BigQuery)
Frequência: Manual (comentar schedule_interval para ativar agendamento diário às 06h00)
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# --- CONSTANTES ---
# Projeto GCP e configurações
PROJECT_ID = "rrdata89"
REGION_BR = "southamerica-east1"
# URL fixa da Cloud Function (Deployada via Cloud Console ou CI/CD)
FUNCTION_URL = "https://cf-extract-api-bitcoin-1013772993221.southamerica-east1.run.app"

# Nomes das Stored Procedures no BigQuery
PROC_TRUSTED = f"{PROJECT_ID}.trusted.SP_PROCESSAR_BITCOIN_V2"
PROC_REFINED = f"{PROJECT_ID}.refined.SP_PROCESSAR_REFINED"

# Configuração padrão para tasks
default_args = {
    'owner': 'rrdata89',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_api_bitcoin_v4',
    default_args=default_args,
    description='Pipeline V4: Extração Bitcoin → RAW → TRUSTED → REFINED',
    # schedule_interval='0 6 * * *',  # DESCOMENTE para executar automaticamente 06h00 Brasil (UTC-3)
    schedule_interval=None,  # EXECUÇÃO MANUAL POR ENQUANTO
    start_date=days_ago(1),
    catchup=False,
    tags=['bitcoin', 'prod', 'refined'],
) as dag:

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TASK 1: EXTRAÇÃO (RAW)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Executa Cloud Function que:
    # 1. Chama API CoinGecko (Bitcoin histórico 1 dia em BRL)
    # 2. Converte resposta para JSON
    # 3. Insere em rrdata89.raw.api_bitcoin com timestamp de ingestão
    t1_raw = BashOperator(
        task_id='cf_extract_api-raw',
        bash_command=f"""
        curl -m 300 -X POST {FUNCTION_URL} \
        -H "Authorization: bearer $(gcloud auth print-identity-token)" \
        -H "Content-Type: application/json" \
        -d '{{}}'
        """
    )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TASK 2: TRANSFORMAÇÃO (TRUSTED)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Executa Stored Procedure que:
    # 1. Extrai dados JSON da camada RAW
    # 2. Converte timestamps e valores (milissegundos → TIMESTAMP, string → FLOAT)
    # 3. Deduplica por data_referencia (mantém última ingestão)
    # 4. Implementa lógica DELTA (insere apenas dados novos)
    # 5. Evita duplicatas com WHERE NOT EXISTS
    t2_trusted = BigQueryInsertJobOperator(
        task_id='sp_proc-trusted',
        configuration={"query": {"query": f"CALL `{PROC_TRUSTED}`();", "useLegacySql": False}},
        location=REGION_BR,
    )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TASK 3: REFINAMENTO (REFINED)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Executa Stored Procedure que:
    # 1. Cria tabela REFINED se não existir (idempotente)
    # 2. Lê dados da camada TRUSTED (dados já validados)
    # 3. Implementa lógica DELTA incremental (apenas novos records)
    # 4. Adiciona timestamp de refino para auditoria
    # 5. Carrega em rrdata89.refined.tb_bitcoin_analitico (consumo final BI)
    t3_refined = BigQueryInsertJobOperator(
        task_id='sp_proc-refined',
        configuration={"query": {"query": f"CALL `{PROC_REFINED}`();", "useLegacySql": False}},
        location=REGION_BR,
    )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ORQUESTRAÇÃO: Sequência de execução
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # RAW → TRUSTED → REFINED (dependência linear)
    t1_raw >> t2_trusted >> t3_refined
