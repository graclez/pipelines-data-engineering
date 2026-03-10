from prefect import flow, task, get_run_logger
import subprocess
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent


@task
def run_dbt_run():
    logger = get_run_logger()
    logger.info("Ejecutando dbt run")
    result = subprocess.run(
        ["dbt", "run"],
        cwd=PROJECT_DIR,
        text=True,
        capture_output=True,
        shell=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt run falló")


@task
def run_dbt_test():
    logger = get_run_logger()
    logger.info("Ejecutando dbt test")
    result = subprocess.run(
        ["dbt", "test"],
        cwd=PROJECT_DIR,
        text=True,
        capture_output=True,
        shell=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt test falló")


@flow(name="pipeline_classicmodels")
def pipeline_classicmodels():
    logger = get_run_logger()
    logger.info("Iniciando pipeline")
    run_dbt_run()
    run_dbt_test()
    logger.info("Pipeline finalizado correctamente")


if __name__ == "__main__":
    pipeline_classicmodels()