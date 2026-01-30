"""
DAG: Pipeline Bitcoin (Cloud Run + Dataform)
Descrição: Orquestração completa: Ingestão (Cloud Run) -> Transformação (Dataform)
Arquitetura: RAW (Cloud Run) → TRUSTED/REFINED (Dataform)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# ==============================================================================
# 🛠️ CONFIGURAÇÕES
# ==============================================================================
PROJECT_ID = "rrdata89"
REGION = "southamerica-east1"
REPOSITORY_ID = "rrdata89" 

# URL da sua Cloud Function (Ingestão Raw)
FUNCTION_URL = "https://cf-api-bitcoin-1013772993221.southamerica-east1.run.app"

# Configuração Padrão do Airflow
default_args = {
    'owner': 'rrdata89',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "pipeline_api_bitcoin_v5_dataform",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily", # Roda diariamente
    catchup=False,
    tags=["bitcoin", "dataform"],
) as dag:

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TASK 1: INGESTÃO (Camada RAW)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Chama a Cloud Function para buscar dados na API e salvar no Storage/BQ
    t1_extract_raw = BashOperator(
        task_id='trigger_cloud_function',
        bash_command=f"""
        curl -m 300 -X POST {FUNCTION_URL} \
        -H "Authorization: bearer $(gcloud auth print-identity-token)" \
        -H "Content-Type: application/json" \
        -d '{{}}'
        """
    )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TASK 2: COMPILAÇÃO (Dataform)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Compila o projeto para garantir que não há erros de código antes de rodar
    t2_compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compilar_projeto",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main", # Pega a versão oficial da branch main
        },
    )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TASK 3: TRANSFORMAÇÃO (Trusted & Refined)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Executa APENAS as tabelas com a tag "bitcoin"
    t3_execute_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="executar_transformacao",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compilar_projeto')['name'] }}",
            "invocation_config": {
                # AQUI ESTÁ O SEGREDO: Roda apenas o que é da API Bitcoin
                "included_tags": ["bitcoin"], 
                "transitive_dependencies_included": False,
                "transitive_dependents_included": False
            }
        },
    )

    # Define a ordem de execução:
    # 1. Roda Cloud Function (Raw)
    # 2. Compila Dataform
    # 3. Roda Dataform (Trusted -> Refined)
    t1_extract_raw >> t2_compile_dataform >> t3_execute_dataform