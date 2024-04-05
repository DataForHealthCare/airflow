import pendulum
from datetime import datetime
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# Set local timezone to ensure we have "Time zone aware DAGs".
local_tz = pendulum.timezone("America/Chicago")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Retrieve the current datetime
current_datetime = datetime.now()
current_date = current_datetime.strftime("%Y-%m-%d")

# Claims driver start & end dates
claims_driver_start_date = current_date
claims_driver_end_date = current_date

# Retrieving the target environment variable
target_environment = Variable.get("environment_short", default_var="dev")

dag = DAG(
    dag_id="claims_driver_orchestration",
    start_date=pendulum.datetime(2024, 3, 11, tz=local_tz),
    schedule_interval="0 11 * * *",
    catchup=False,
    default_args=default_args,
    tags=["standardized", "ADWHICORE"],
    is_paused_upon_creation=False
)
with dag:
    admin_systems_ingestion_trigger_dates = DatabricksSubmitRunOperator(
        task_id="ingestion_dates",
        databricks_conn_id="Databricks",
        notebook_task={
            'notebook_path': "/Workspace/Users/f9656@healthpartners.com/orchestration_validation",
            'base_parameters': {
                "source_environment": "prd",
                "target_environment": "dev"
            }
        },
        existing_cluster_id="1006-181204-l3w0yie3"
    )

    @task(task_id=f"admin_systems_dates")
    def get_notebook_output(ti=None):
        run_id = ti.xcom_pull("ingestion_dates", key="run_id")
        databricks_hook = DatabricksHook(databricks_conn_id="Databricks")
        result = databricks_hook._do_api_call(("GET", f"api/2.1/jobs/runs/get-output?run_id={run_id}"), {})    
        output_run = result["notebook_output"]
        return output_run
    get_notebook_output = get_notebook_output()

    @task.short_circuit(task_id=f"evaluate_adminsystems_tables", ignore_downstream_trigger_rules=False)
    def evaluate_admin_tables(ti=None):
        trigger_dates_result = ti.xcom_pull("admin_systems_dates", key="return_value")
        result = trigger_dates_result["result"]
        if result == "The ingestion dates for all AdminSystems tables are out-of-sync. The claims driver workflow will not be started.":
            Variable.set(key="adminsystem_stats", value={"adminsystem_validation": "AdminSystem tables are out-of-sync."}, serialize_json=True)
            Variable.set(key="monitor_data", value={"adminsystem_validation": "AdminSystem tables are out-of-sync."}, serialize_json=True)
            return False
        if result == "All AdminSystems tables have current ingestion dates. The claims driver workflow can be initiated.":
            return True         
    evaluate_admin_tables = evaluate_admin_tables()
            
    claims_driver_workflow = DatabricksSubmitRunOperator(
        task_id="load_table",
        databricks_conn_id="Databricks",
        notebook_task={
            'notebook_path': "/Repos/f9656@healthpartners.com/Data-Ingestion-Pipeline/Databricks/ProductTeam/HICORE/claims_driver/claims_driver_main",
            'base_parameters': {
                "source_environment": "prd",
                "target_environment": "dev",
                "claims_driver_start_date": claims_driver_start_date,
                "claims_driver_end_date": claims_driver_end_date
            }
        },
        existing_cluster_id="1006-181204-l3w0yie3",
        do_xcom_push = True
    )

    @task(task_id=f"orchestration_monitoring_table")
    def capture_monitoring_metrics(ti=None):
        run_id = ti.xcom_pull("load_table", key="run_id")
        databricks_hook = DatabricksHook(databricks_conn_id="Databricks")
        result = databricks_hook._do_api_call(("GET", f"api/2.1/jobs/runs/get-output?run_id={run_id}"), {})   
        return result
    capture_monitoring_metrics = capture_monitoring_metrics()
    
    @task(task_id=f"collective_driver_data")
    def load_monitoring_table(ti=None):
        task_id = "orchestration_monitoring_table"
        request = ti.xcom_pull(task_id, key="return_value")
        monitoring_statistics = {
            "result_state_status": request["metadata"]["state"]["result_state"],
            "start_time": request["metadata"]["start_time"],
            "end_time": request["metadata"]["end_time"],
            "run_nm": request["metadata"]["run_name"],
            "run_url": request["metadata"]["run_page_url"],
            "created_nm": request["metadata"]["creator_user_name"]
        }  
        return monitoring_statistics
    load_monitoring_table = load_monitoring_table()  

    @task(task_id="xcoms_data")
    def pull_driver_data(ti=None):
        monitoring_data_request = ti.xcom_pull("collective_driver_data", key="return_value")
        Variable.set(key="monitor_data", value=monitoring_data_request, serialize_json=True)
        return monitoring_data_request
    pull_driver_data = pull_driver_data()

    monitoring_table_workflow = DatabricksSubmitRunOperator(
        task_id="load_monitoring_table",
        databricks_conn_id="Databricks",
        notebook_task={
            'notebook_path': "/Repos/f9656@healthpartners.com/Data-Ingestion-Pipeline/Databricks/ProductTeam/HICORE/claims_driver_monitor/claims_driver_monitor",
            'base_parameters': {
                "source_environment": "prd",
                "target_environment": "dev",
                "monitoring_statistics": Variable.get("monitor_data", default_var={"value": "workflow failed"}),
                "adminsystem_stats": Variable.get("adminsystem_stats", default_var={"value": "AdminSystem tables are in-sync."})
            }
        },
        existing_cluster_id="1006-181204-l3w0yie3",
        do_xcom_push = True,
        trigger_rule=TriggerRule.NONE_FAILED
    )
    admin_systems_ingestion_trigger_dates >> get_notebook_output >> evaluate_admin_tables >> claims_driver_workflow >> capture_monitoring_metrics >> load_monitoring_table >> pull_driver_data >> monitoring_table_workflow 
