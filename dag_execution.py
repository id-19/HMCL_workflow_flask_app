"""
DAG Execution Module

This module provides functionality to trigger and monitor Airflow DAG runs
programmatically with support for both REST API and CLI methods.
"""
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Dict, Optional, Any, Tuple

import requests
from airflow import settings
from airflow.models.dagrun import DagRun
from airflow.utils.session import provide_session
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logger
logger = logging.getLogger("DagExecutor")

"""
How to use REST API to trigger DAGS(or do anything else with Airflow):
Request

ENDPOINT_URL="http://localhost:8080/"
curl -X POST ${ENDPOINT_URL}/auth/token \
  -H "Content-Type: application/json" \
  -d '{
    "username": "your-username",
    "password": "your-password"
  }'
Response

{
  "access_token": "<JWT-TOKEN>"
}
Use the JWT token to call Airflow public API

ENDPOINT_URL="http://localhost:8080/"
curl -X GET ${ENDPOINT_URL}/api/v2/dags \
  -H "Authorization: Bearer <JWT-TOKEN>"

"""

def auth() -> str:
    """
    Authenticate with Airflow and return JWT token.
    
    Returns:
        str: JWT access token
        
    Raises:
        requests.exceptions.RequestException: If authentication fails
    """
    endpoint_url = "http://localhost:8080/"
    username = os.getenv("AIRFLOW_USERNAME")
    password = os.getenv("AIRFLOW_PASSWORD")
    
    if not username or not password:
        raise ValueError("AIRFLOW_USERNAME and AIRFLOW_PASSWORD must be set in environment variables")
        
    response = requests.post(
        f"{endpoint_url}/auth/token",
        json={"username": username, "password": password},
        timeout=10
    )
    response.raise_for_status()
    return response.json()["access_token"]


def run_dag_with_monitoring(
    dag_id: str,
    run_id: Optional[str] = None,
    conf: Dict[str, Any] = {},
    logical_date: Optional[str] = None,
    wait: bool = True,
    timeout: int = 300,
    poll_interval: float = 5.0
) -> Dict[str, Any]:
    """
    Trigger Airflow DAG with full configuration and optional wait for completion
    
    Args:
        dag_id (str): Name of DAG to execute
        run_id (Optional[str]): Custom execution ID (name_run by default)
        conf (Optional[Dict]): Configuration parameters
        wait (bool): Whether to wait for DAG completion
        timeout (int): Max seconds to wait
        poll_interval (float): Poll interval for status checking

    Returns:
        Dict: {
            "execution_date": str,
            "external_trigger": bool,
            "state": str,
            "end_date": Optional[str]
        }
        
    Raises:
        ValueError: Missing required parameters
        TimeoutError: Exceeds timeout
    """
    try:
        #Validate Inputs
        if dag_id.strip() == "":
            raise ValueError("DAG ID cannot be empty")
            
        #Trigger DAG
        logger.info(f"Triggering DAG: {dag_id} with conf {conf} and run_id {run_id}")
        
        dag_run = None
        #Trigger DAG using API
        try:
            token = auth()
            payload = {
                "conf": conf,
                "logical_date": logical_date or datetime.now().isoformat() + "+05:30",
                "dag_run_id": run_id or f"{dag_id}_run_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            }
            airflow_host = os.getenv("AIRFLOW_HOST") or "http://localhost"
            airflow_port = os.getenv("AIRFLOW_PORT") or "8080"
            dag_run = requests.post(f"{airflow_host}:{airflow_port}/api/v2/dags/{dag_id}/dagRuns", json=payload,
            headers={"Authorization": f"Bearer {token}"},
            data = json.dumps(payload)
            )
            if "detail" in dag_run.json():
                raise Exception(f"Running DAG {dag_id} failed with error: {dag_run.json()["detail"]}")
            dag_run = dag_run.json()
            if dag_run["state"] != "queued":
                raise ValueError(f"Failed to trigger DAG using REST api: {dag_run}")
        except Exception as e:
            logger.error(f"Failed to trigger DAG: {str(e)}")
            # Try triggering DAG using CLI instead
            try:
                run_id = run_id or f"{dag_id}_run_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                airflow_path: str = os.getenv("AIRFLOW_PATH") if os.getenv("AIRFLOW_PATH") is not None else "airflow" # type:ignore
                dag_run = subprocess.run(
                    [
                        # "airflow",
                        airflow_path,
                         "dags", 
                         "trigger",
                         dag_id,
                         "--run-id",
                         run_id,
                         "--conf",
                         json.dumps(conf)
                    ],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                if dag_run.returncode != 0:
                    """ Print shell output for debugging"""
                    print(dag_run.stdout.decode('utf-8'))
                    print("\n\n----\n\n")
                    print(dag_run.stderr.decode('utf-8'))
                    raise ValueError(f"Failed to trigger DAG using CLI: {dag_run.stderr.decode('utf-8')}")
            except Exception as e:
                logger.error(f"Failed to trigger DAG: {str(e)}")
                raise

        
        if not isinstance(dag_run, dict):
            logger.info(f"DAG run thru CLI: {dag_run}")
            return _format_response(dag_id, run_id or "unknown", {"state": "unknown"}, datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        #Optional Waiting
        if not run_id and isinstance(dag_run, dict):
            run_id = dag_run["dag_run_id"]
        try:
            run_id = str(run_id)
        except Exception as e:
            logger.error(f"Failed to convert run_id to string: {str(e)}")
            raise
        if wait:
            logger.info(f"Monitoring DAG {dag_id} execution")
            result, end_time = wait_for_dag_completion(
                run_id, 
                timeout=timeout,
                poll_interval=poll_interval
            )
        else:
            result = get_dag_run_status(run_id)
            end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        return _format_response(dag_id, run_id, result, end_time)

    except Exception as e:
        logger.error(f"DAG execution failed:\n {str(e)}")
        return _format_response(dag_id, run_id or "unknown", {"state": "failed"}, datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

# Internal Monitoring Helper Functions
@provide_session
def wait_for_dag_completion(dag_run_id: str, 
                           timeout: float = 300,
                           poll_interval: float = 5, 
                           session=None) -> Tuple[Dict, str]:
    """
    Poll until DAG completes or times out
    
    Returns:
        Tuple[Dict, str]: Contains the DAG run status with completion time
    """
    end_time = time.time() + timeout
    while time.time() < end_time:
        status = get_dag_run_status(dag_run_id, session=session)
        if status["state"] in ("success", "failed"):
            # Add completion time to the status
            status["end_date"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            return status, datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        time.sleep(poll_interval)
    raise TimeoutError("DAG execution timed out")

@provide_session
def get_dag_run_status(dag_run_id: str, session=None) -> Dict:
    """Retrieve current DAG execution state"""
    if session is None:
        session = settings.Session()
    try:
        dr = session.query(DagRun).filter(
            # DagRun.external_trigger==True,
            DagRun.run_id==dag_run_id,
            # DagRun.conf==conf
        ).first()
        return {
            "state": dr.state,
            # "metadata": dr.metadata,
            "queued_at": dr.queued_at.strftime("%Y-%m-%d %H:%M:%S"), # convert datetime object to str
            "run_type": dr.run_type,
        }
    except Exception as e:
        logger.error(f"State retrieval failed: {str(e)}")
        raise

def _format_response(dag_id: str, run_id: str, result: Dict[str, Any], end_time: str) -> Dict[str, Any]:
    """
    Format the DAG run response consistently with the most relevant fields.
    
    Args:
        dag_id: The DAG ID
        run_id: The run ID
        result: Dictionary containing DAG run details from Airflow API
        end_time: Timestamp when the DAG run completed
        
    Returns:
        Dict[str, Any]: Contains the relevant fields from the API response:
            - dag_run_id: Unique ID of the DAG run
            - dag_id: The DAG ID
            - state: Current state of the DAG run
            - start_date: When the DAG run started
            - end_date: When the DAG run completed
            - logical_date: The logical date of the DAG run
    """
    return {
        "dag_run_id": result.get("dag_run_id") or run_id,
        "dag_id": dag_id,
        "state": result.get("state"),
        "start_date": result.get("start_date"),
        "end_date": end_time,
        "logical_date": result.get("logical_date")
    }

# Example Usage
if __name__ == "__main__":
    try:
        # Trigger DAG with parameters
        result = run_dag_with_monitoring(
            dag_id="simple_param_dag",
            # dag_id="Sample DAG with Display Name",
            run_id=f"custom_run_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            conf={"message": "Hello World"},
            wait=True,
            timeout=600
        )
        
        print(f"DAG Status: {result['state']}")
        print(f"Execution Details: {json.dumps(result, indent=2)}")

    except TimeoutError:
        print("DAG execution exceeded timeout")
    except Exception as e:
        print(f"DAG failure: {str(e)}")
        exit(1)


"""
Response on successfully triggering a DAG:
{
  "dag_run_id": "string",
  "dag_id": "string",
  "logical_date": "2019-08-24T14:15:22Z",
  "queued_at": "2019-08-24T14:15:22Z",
  "start_date": "2019-08-24T14:15:22Z",
  "end_date": "2019-08-24T14:15:22Z",
  "data_interval_start": "2019-08-24T14:15:22Z",
  "data_interval_end": "2019-08-24T14:15:22Z",
  "run_after": "2019-08-24T14:15:22Z",
  "last_scheduling_decision": "2019-08-24T14:15:22Z",
  "run_type": "backfill",
  "state": "queued",
  "triggered_by": "cli",
  "conf": {},
  "note": "string",
  "dag_versions": [
    {
      "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
      "version_number": 0,
      "dag_id": "string",
      "bundle_name": "string",
      "bundle_version": "string",
      "created_at": "2019-08-24T14:15:22Z",
      "bundle_url": "string"
    }
  ],
  "bundle_version": "string"
}
"""

"""
Response on failure to trigger dag:
{
  "detail": "string"
}
"""