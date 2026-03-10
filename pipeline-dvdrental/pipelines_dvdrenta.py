import os
import time
import subprocess
from pathlib import Path
from typing import Optional, Dict, Any

import httpx
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task


# ---------------------------------------------------------
# Configuración de rutas y carga de entorno
# ---------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)

AIRBYTE_BASE_URL = os.getenv("AIRBYTE_BASE_URL", "http://localhost:8000").rstrip("/")
AIRBYTE_CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")

DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", str(BASE_DIR))).resolve()
DBT_PROFILES_DIR = Path(os.getenv("DBT_PROFILES_DIR", str(BASE_DIR))).resolve()
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")

DBT_PROFILE_NAME = os.getenv("DBT_PROFILE_NAME", "tpfinal_dbt")
DBT_TARGET_NAME = os.getenv("DBT_TARGET_NAME", "dev")

PUBLIC_API_BASE = f"{AIRBYTE_BASE_URL}/api/public/v1"


# ---------------------------------------------------------
# Validación de entorno
# ---------------------------------------------------------
def validate_env() -> None:
    required_vars = {
        "AIRBYTE_CLIENT_ID": AIRBYTE_CLIENT_ID,
        "AIRBYTE_CLIENT_SECRET": AIRBYTE_CLIENT_SECRET,
        "AIRBYTE_CONNECTION_ID": AIRBYTE_CONNECTION_ID,
        "MOTHERDUCK_TOKEN": MOTHERDUCK_TOKEN,
    }

    missing = [key for key, value in required_vars.items() if not value]

    if missing:
        raise ValueError(
            f"Faltan variables de entorno requeridas: {', '.join(missing)}"
        )

    if not DBT_PROJECT_DIR.exists():
        raise FileNotFoundError(f"DBT_PROJECT_DIR no existe: {DBT_PROJECT_DIR}")

    if not DBT_PROFILES_DIR.exists():
        raise FileNotFoundError(f"DBT_PROFILES_DIR no existe: {DBT_PROFILES_DIR}")


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def _clean_secret(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    return value.replace('"', "").replace("'", "").strip()


def _dbt_env() -> Dict[str, str]:
    env = os.environ.copy()

    token = _clean_secret(os.getenv("MOTHERDUCK_TOKEN"))
    if token:
        env["MOTHERDUCK_TOKEN"] = token
        env["DBT_ENV_CUSTOM_ENV_MOTHERDUCK_TOKEN"] = token

    env["DBT_PROJECT_DIR"] = str(DBT_PROJECT_DIR)
    env["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)
    return env


def _run_command(
    dbt_args: list[str],
    timeout_seconds: int = 3600,
) -> Dict[str, Any]:
    logger = get_run_logger()
    env = _dbt_env()

    command = ["dbt"] + dbt_args + [
        "--profiles-dir", str(DBT_PROFILES_DIR),
        "--profile", DBT_PROFILE_NAME,
        "--target", DBT_TARGET_NAME,
    ]

    logger.info(
        "Ejecutando comando dbt: %s",
        " ".join(command[:4]) + " ... (parámetros sensibles ocultos)"
    )

    start_time = time.time()

    try:
        result = subprocess.run(
            command,
            cwd=str(DBT_PROJECT_DIR),
            env=env,
            text=True,
            capture_output=True,
            shell=True,   # útil en Windows / PowerShell
            timeout=timeout_seconds
        )

        elapsed = round(time.time() - start_time, 2)

        if result.stdout:
            logger.info("[dbt stdout]\n%s", result.stdout)

        if result.stderr:
            logger.warning("[dbt stderr]\n%s", result.stderr)

        logger.info(
            "Comando dbt finalizado. Exit code=%s, duración=%ss",
            result.returncode,
            elapsed
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Error ejecutando dbt {' '.join(dbt_args)}. "
                f"Exit code: {result.returncode}"
            )

        return {
            "status": "success",
            "command": " ".join(dbt_args),
            "return_code": result.returncode,
            "duration_seconds": elapsed
        }

    except subprocess.TimeoutExpired as exc:
        logger.exception("Timeout ejecutando dbt %s", " ".join(dbt_args))
        raise RuntimeError(
            f"Timeout ejecutando dbt {' '.join(dbt_args)}"
        ) from exc

    except Exception:
        logger.exception("Fallo inesperado ejecutando dbt %s", " ".join(dbt_args))
        raise


# ---------------------------------------------------------
# Tareas de validación
# ---------------------------------------------------------
@task(name="Validate Environment", retries=0)
def validate_environment_task() -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Validando variables de entorno y rutas del proyecto")
    validate_env()
    logger.info("Validación de entorno completada correctamente")
    return {"status": "success"}


# ---------------------------------------------------------
# Tareas Airbyte
# ---------------------------------------------------------
@task(
    name="Extract and Load (Airbyte)",
    retries=3,
    retry_delay_seconds=30,
    timeout_seconds=1800
)
def run_airbyte_sync() -> Dict[str, Any]:
    logger = get_run_logger()
    start_time = time.time()

    auth_payload = {
        "client_id": AIRBYTE_CLIENT_ID,
        "client_secret": AIRBYTE_CLIENT_SECRET,
        "grant-type": "client_credentials",
    }

    try:
        with httpx.Client(timeout=30) as client:
            logger.info("Solicitando token de autenticación de Airbyte")
            token_res = client.post(
                f"{AIRBYTE_BASE_URL}/api/v1/applications/token",
                json=auth_payload
            )
            token_res.raise_for_status()

            access_token = token_res.json().get("access_token")
            if not access_token:
                raise RuntimeError("Airbyte no devolvió access_token")

            headers = {"Authorization": f"Bearer {access_token}"}

            logger.info("Disparando sincronización Airbyte")
            sync_res = client.post(
                f"{PUBLIC_API_BASE}/jobs",
                headers=headers,
                json={
                    "connectionId": AIRBYTE_CONNECTION_ID,
                    "jobType": "sync"
                }
            )
            sync_res.raise_for_status()

            job_id = sync_res.json().get("jobId")
            if not job_id:
                raise RuntimeError("Airbyte no devolvió jobId")

            logger.info("Sincronización iniciada correctamente. job_id=%s", job_id)

            max_wait_seconds = 1800
            poll_interval_seconds = 10
            waited = 0

            while waited < max_wait_seconds:
                status_res = client.get(
                    f"{PUBLIC_API_BASE}/jobs/{job_id}",
                    headers=headers
                )
                status_res.raise_for_status()

                status = status_res.json().get("status", "").lower()
                logger.info("Estado actual del job Airbyte %s: %s", job_id, status)

                if status in ["succeeded", "completed"]:
                    elapsed = round(time.time() - start_time, 2)
                    logger.info(
                        "Job Airbyte completado correctamente. job_id=%s duración=%ss",
                        job_id,
                        elapsed
                    )
                    return {
                        "status": "success",
                        "job_id": job_id,
                        "airbyte_status": status,
                        "duration_seconds": elapsed
                    }

                if status in ["failed", "cancelled"]:
                    raise RuntimeError(
                        f"Job Airbyte terminó en estado '{status}'. job_id={job_id}"
                    )

                time.sleep(poll_interval_seconds)
                waited += poll_interval_seconds

            raise TimeoutError(
                f"Timeout esperando finalización del job Airbyte {job_id}"
            )

    except httpx.HTTPStatusError as exc:
        logger.exception("Error HTTP en Airbyte")
        raise RuntimeError(
            f"Error HTTP en Airbyte: {exc.response.status_code}"
        ) from exc

    except Exception:
        logger.exception("Fallo en la tarea de sincronización Airbyte")
        raise


# ---------------------------------------------------------
# Tareas dbt
# ---------------------------------------------------------
@task(
    name="dbt deps",
    retries=2,
    retry_delay_seconds=15,
    timeout_seconds=900
)
def dbt_deps() -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Ejecutando dbt deps")
    return _run_command(["deps"], timeout_seconds=900)


@task(
    name="dbt run",
    retries=2,
    retry_delay_seconds=20,
    timeout_seconds=3600
)
def transform(select: Optional[str] = None) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Ejecutando dbt run")

    args = ["run"]
    if select:
        args.extend(["--select", select])
        logger.info("dbt run con selector: %s", select)

    return _run_command(args, timeout_seconds=3600)


@task(
    name="dbt test",
    retries=1,
    retry_delay_seconds=15,
    timeout_seconds=3600
)
def test_data(select: Optional[str] = None) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Ejecutando dbt test")

    args = ["test"]
    if select:
        args.extend(["--select", select])
        logger.info("dbt test con selector: %s", select)

    return _run_command(args, timeout_seconds=3600)


@task(
    name="dbt docs generate",
    retries=1,
    retry_delay_seconds=15,
    timeout_seconds=1800
)
def generate_docs() -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Generando documentación dbt")
    return _run_command(["docs", "generate"], timeout_seconds=1800)


# ---------------------------------------------------------
# Flow principal
# ---------------------------------------------------------
@flow(name="Dvdrental ELT Pipeline")
def ecommerce_pipeline(
    run_extract: bool = True,
    run_transform: bool = True,
    run_tests: bool = True,
    run_docs: bool = False,
    dbt_select: Optional[str] = None,
    validate_connections: bool = False
) -> Dict[str, Any]:
    logger = get_run_logger()
    pipeline_start = time.time()

    logger.info("==============================================")
    logger.info("Iniciando pipeline ELT de Dvdrental")
    logger.info("run_extract=%s", run_extract)
    logger.info("run_transform=%s", run_transform)
    logger.info("run_tests=%s", run_tests)
    logger.info("run_docs=%s", run_docs)
    logger.info("dbt_select=%s", dbt_select)
    logger.info("validate_connections=%s", validate_connections)
    logger.info("==============================================")

    summary: Dict[str, Any] = {
        "status": "running",
        "steps": {}
    }

    try:
        summary["steps"]["validate_environment"] = validate_environment_task()

        if run_extract:
            logger.info("Paso 1: extracción y carga desde Airbyte")
            summary["steps"]["airbyte_sync"] = run_airbyte_sync()
        else:
            logger.info("Paso 1 omitido: extracción y carga")
            summary["steps"]["airbyte_sync"] = {"status": "skipped"}

        if run_transform:
            logger.info("Paso 2: instalación de dependencias dbt")
            summary["steps"]["dbt_deps"] = dbt_deps()

            logger.info("Paso 3: transformación con dbt")
            summary["steps"]["dbt_run"] = transform(select=dbt_select)
        else:
            logger.info("Paso 2/3 omitidos: transformaciones dbt")
            summary["steps"]["dbt_deps"] = {"status": "skipped"}
            summary["steps"]["dbt_run"] = {"status": "skipped"}

        if run_tests:
            logger.info("Paso 4: pruebas de calidad con dbt")
            summary["steps"]["dbt_test"] = test_data(select=dbt_select)
        else:
            logger.info("Paso 4 omitido: pruebas de calidad")
            summary["steps"]["dbt_test"] = {"status": "skipped"}

        if run_docs:
            logger.info("Paso 5: generación de documentación dbt")
            summary["steps"]["dbt_docs"] = generate_docs()
        else:
            logger.info("Paso 5 omitido: documentación dbt")
            summary["steps"]["dbt_docs"] = {"status": "skipped"}

        elapsed = round(time.time() - pipeline_start, 2)
        summary["status"] = "success"
        summary["duration_seconds"] = elapsed

        logger.info("==============================================")
        logger.info("Pipeline completado correctamente en %ss", elapsed)
        logger.info("Resumen final: %s", summary)
        logger.info("==============================================")

        return summary

    except Exception as exc:
        elapsed = round(time.time() - pipeline_start, 2)
        summary["status"] = "failed"
        summary["duration_seconds"] = elapsed
        summary["error"] = str(exc)

        logger.exception("Pipeline falló luego de %ss", elapsed)
        logger.error("Resumen final: %s", summary)

        raise


# ---------------------------------------------------------
# Ejecución local / despliegue
# ---------------------------------------------------------
if __name__ == "__main__":
    ecommerce_pipeline.serve(
        name="dvdrental-daily",
        cron="0 6 * * *",
        parameters={
            "run_extract": True,
            "run_transform": True,
            "run_tests": True,
            "run_docs": False,
            "dbt_select": None,
            "validate_connections": False
        }
    )