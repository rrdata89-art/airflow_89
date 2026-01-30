"""
DAG: Pipeline Bitcoin V5 (Dataform)
Descrição: Ingestão (Cloud Run) -> Transformação (Dataform)
Frequência: Manual (Sob demanda)
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
REPOSITORY_ID = "rrdara89" 

# URL da Cloud Function
FUNCTION_URL = "https://cf-api-bitcoin-1013772993221.southamerica-east1.run.app"

default_args = {
    'owner': 'rrdata89',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "pipeline_api_bitcoin_dataform",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, # Manual
    catchup=False,
    tags=["bitcoin", "dataform"],
) as dag:

    # TASK 1: INGESTÃO
    t1_extract_raw = BashOperator(
        task_id='trigger_cloud_function',
        bash_command=f"""
        curl -m 300 -X POST {FUNCTION_URL} \
        -H "Authorization: bearer $(gcloud auth print-identity-token)" \
        -H "Content-Type: application/json" \
        -d '{{}}'
        """
    )

    # TASK 2: COMPILAÇÃO
    t2_compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compilar_projeto",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
        },
    )

    # TASK 3: EXECUÇÃO
    t3_execute_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="executar_transformacao",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compilar_projeto')['name'] }}",
            "invocation_config": {
                "included_tags": ["bitcoin"],
                "transitive_dependencies_included": False,
                "transitive_dependents_included": False
            }
        },
    )

    t1_extract_raw >> t2_compile_dataform >> t3_execute_dataform