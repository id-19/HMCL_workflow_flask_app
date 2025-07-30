import json
import flask
from dag_execution import run_dag_with_monitoring
from request_types import AuthenticationRequest, ExecuteDatabaseWorkflowRequest, ExecuteMailWorkflowRequest, ExecuteDagRequest

# For Auth
import os
from dotenv import load_dotenv
load_dotenv()

# Define a generic route for DAG execution
# Then define a path each for the database workflow and the mail workflow
# TODO: Remember to put the dags in the dag folder, just leaving them in the same folder as the app will not work
app = flask.Flask(__name__)

@app.route("/execute_dag/<dag_id>", methods=["POST"])
def execute_dag(dag_id: str, wait_for_termination: bool = False):
    try:
        params = flask.request.get_json()
    except Exception as e:
        return {"error": f"Failed to parse request: {str(e)}"}, 400
    # Validate params
    try:
        params = ExecuteDagRequest(**params)
    except Exception as e:
        return {"error": f"Failed to validate request: {str(e)}"}, 400
    try:
        run_dag_with_monitoring(dag_id, wait=wait_for_termination, conf=params.conf)
    except Exception as e:
        return {"error": f"Failed to execute DAG: {str(e)}"}, 500
    return {"message": "DAG executed successfully"}, 200

@app.route("/execute_database_workflow", methods=["POST"])
def execute_database_workflow():
    try:
        params = flask.request.get_json()
    except Exception as e:
        return {"error": f"Failed to parse request: {str(e)}"}, 400
    # Validate params
    try:
        params = ExecuteDatabaseWorkflowRequest(**params)
    except Exception as e:
        return {"error": f"Failed to validate request: {str(e)}"}, 400
    try:
        run_dag_with_monitoring("api_sap_workflow", conf=params.model_dump())
    except Exception as e:
        return {"error": f"Failed to execute database workflow: {str(e)}"}, 500
    return {"message": "Database workflow executed successfully"}, 200

@app.route("/execute_mail_workflow", methods=["POST"])
def execute_mail_workflow():
    try:
        params = flask.request.get_json()
    except Exception as e:
        return {"error": f"Failed to parse request: {str(e)}"}, 400
    # Validate params
    try:
        params = ExecuteMailWorkflowRequest(**params)
    except Exception as e:
        return {"error": f"Failed to validate request: {str(e)}"}, 400
    try:
        run_dag_with_monitoring("dynamic_email_sender", conf=params.model_dump())
    except Exception as e:
        return {"error": f"Failed to execute mail workflow: {str(e)}"}, 500
    return {"message": "Mail workflow executed successfully"}, 200

@app.route("/authenticate", methods=["POST"])
def authenticate():
    # TODO: Implement authentication, this is google auth for storing creds, NOT RUNNING A DAG
    try:
        params = flask.request.get_json()
    except Exception as e:
        return {"error": f"Failed to parse request: {str(e)}"}, 400
    # Validate params
    try:
        params = AuthenticationRequest(**params)
    except Exception as e:
        return {"error": f"Failed to validate request: {str(e)}"}, 400
    try:
        # TODO: IMPLEMENT AUTH FLOW
        pass
        return {"message": "Authentication not implemented, yet"}, 200
        # return {"message": "Authentication successful"}, 200 # TODO: Uncomment this when implemented
    except Exception as e:
        return {"error": f"Failed to authenticate: {str(e)}"}, 500

    
def create_app():
    # app = flask.Flask(__name__)
    # Initialize auth data with app context
    with app.app_context():
        load_auth_data()
    
    return app

def load_auth_data():
    """Load auth data within app context"""
    auth_file = os.getenv("EMAIL_AUTH_FILE")
    # assert auth_file is not None, "EMAIL_AUTH_FILE environment variable is not set"

    # Create the auth file if it doesn't exist
    if auth_file is None:
        auth_file = "./auth.json"
        app.config['USER_TOKENS'] = {}
        app.config['AUTH_DATA'] = {}
    else:
        with open(auth_file, "r") as f:
            auth_data = json.load(f)
        assert isinstance(auth_data, dict), "EMAIL_AUTH_FILE is not a valid JSON file"
        
        user_tokens = auth_data.get("user_tokens", {})
    
        # Store in current app config
        app.config['USER_TOKENS'] = user_tokens
        app.config['AUTH_DATA'] = auth_data

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
    