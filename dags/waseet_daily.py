"""The daily scan DAG.

A skeleton with the imports you will need and the shape of the run. Fill in the
tasks, wire the dependencies, and delete the comments as you replace them.

Inside the container the paths are:

    /opt/airflow/data       the scan files and lookups
    /opt/airflow/pipeline   pipeline.py and quality.py

Iterate with `dags test`, which runs the whole DAG in one process and prints
straight to your terminal - no unpausing, no waiting for the scheduler:

    docker exec waseet-airflow airflow dags test waseet_daily 2026-05-04
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
import pendulum

DATA = "/opt/airflow/data"
CODE = "/opt/airflow/pipeline"

# TODO. Which failures here deserve a retry, and how many? A supplier that is
# late is not a transient failure, and a renamed column is not one either.
default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def alert(context):
    """TODO. Fires after the retries are exhausted.

    Write a line an on-call engineer can route from. Which task failed matters
    more than that something did: the sensor timing out is the supplier's
    problem, the load failing is yours, and the two wake up different people.
    """
    raise NotImplementedError


def choose_branch(ds):
    """TODO. Return the task_id to run next.

    The 15th of May is Eid. The network is closed, the file arrives with a
    header and nothing under it, and failing that morning would be an alert on a
    day when nothing is wrong. Everything below the branch that is not chosen
    goes to `skipped`, which is why the task that joins the two paths back
    together needs a trigger rule that is not the default.
    """
    raise NotImplementedError


with DAG(
    dag_id="waseet_daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["waseet"],
) as dag:

    # TODO. Wait for the file rather than failing on its absence.
    #
    # poke_interval, timeout, and mode. The 10th of May never arrives at all, so
    # the timeout is not hypothetical - decide what it should be and what should
    # happen when it fires. Use mode="reschedule" and be able to say why.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=DATA + "/scans_{{ ds }}.csv",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60,
        mode="reschedule",
    )

    # TODO. The rest.
    #
    #   choose        BranchPythonOperator on choose_branch
    #   run_pipeline  BashOperator -> cd CODE && python pipeline.py --date {{ ds }}
    #   skip_day      the other side of the branch
    #   run_quality   the suite, after the load, on the warehouse
    #   finish        the join. Its trigger_rule is the thing to get right.

    wait_for_file
