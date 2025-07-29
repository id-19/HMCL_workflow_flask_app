# This file hits the flask app endpoints to test the workflow "simple_param_dag"

import requests
# Run a generic DAG with parameters
server_url = "http://localhost"
server_port = "5000"
dag_id = "simple_param_dag"
params = {
    "dag_id": dag_id,
    "wait_for_termination": True,
    "conf": {
        "message": "Hello World"
    }
}

try:
    response = requests.post(f"{server_url}:{server_port}/execute_dag/{dag_id}", json=params)
    print(response.json())
except Exception as e:
    print(f"Server Error: {str(e)}")
