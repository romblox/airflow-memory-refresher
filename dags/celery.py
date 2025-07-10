from time import sleep

from airflow.sdk import dag, task


# @dag(
#     schedule=None,
#     start_date=None,
#     catchup=False,
# )
@dag
def celery_test_dag():
    @task()
    def a():
        sleep(2)

    @task()
    def b():
        sleep(5)

    # @task(queue="high_cpu_queue")
    @task()
    def c():
        sleep(5)

    @task()
    def d():
        sleep(5)

    @task()
    def e():
        sleep(5)

    # @task(queue="high_cpu_queue")
    @task()
    def f():
        sleep(2)

    a() >> [b(), c(), d(), e()] >> f()

celery_test_dag()
