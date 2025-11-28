from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
    schedule_interval=None,
    start_date=days_ago(0),
    catchup=False,
    tags=["test"]
)
def my_test_dag1():
    @task
    def print_conf():
        from airflow.operators.python import get_current_context
        context = get_current_context()
        conf = context['dag_run'].conf

        print("Received conf:", conf)
        print("param1:", conf.get("param1"))
        print("param2:", conf.get("param2"))

    print_conf()

my_test_dag1 = my_test_dag1()
