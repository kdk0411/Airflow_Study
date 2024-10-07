import pprint as pp
import airflow.utils.dates
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagrunOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "ower": "airflow",
    "start_date": airflow.utils.dates.days_
}


def conditionally_trigger(context, dag_run_obj):
    if context['params']['condition_param']:
        dag_run_obj.payload = {
            'message': context['params']['message']
        }
        pp.pprint(dag_run_obj.payload)
        return dag_run_obj


with DAG(dag_id="triggerdagop_controller_dag", default_args=default_args) as dag:
    trigger = TriggerDagrunOperator(
        task_id="trigger_dag",
        trigger_dag_id="triggerdagop_target_dag",
        provide_context=True,
        python_callable=conditionally_trigger,
        params={
            'condition_param': True,
            'message': 'Hi from the controller'
        },
    )
    last_task = DummyOperator(task_id="last_task")

    trigger >> last_task
