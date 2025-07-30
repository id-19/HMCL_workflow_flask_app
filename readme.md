# Airflow Workflow Automation App

## Project Overview

This project is a Flask-based web application designed to automate workflows by triggering Airflow DAGs. It provides a set of RESTful APIs to initiate and monitor two primary workflows: an email sending workflow and an SAP data synchronization workflow.

## Modules Description

### Core Application

-   **`flask_app.py`**: This is the main entry point of the web application. It sets up a Flask server with the following endpoints:
    -   `/execute_dag/<dag_id>`: A generic endpoint to trigger any DAG by its ID.
    -   `/execute_database_workflow`: A dedicated endpoint to trigger the SAP data workflow.
    -   `/execute_mail_workflow`: A dedicated endpoint to trigger the email sending workflow.
    -   `/authenticate`: A placeholder endpoint for future authentication implementation.

-   **`dag_execution.py`**: This module contains the core logic for interacting with Airflow. It attempts to trigger DAGs using the Airflow REST API and falls back to using the Airflow CLI if the API call fails. It also includes functions to monitor the status of a DAG run and wait for its completion.

### Workflows

#### 1. Email Workflow

-   **`dags/email_workflow_dag/send_email_workflow.py`**: This file defines the Airflow DAG (`dynamic_email_sender`) for the email workflow. The DAG is designed to be triggered dynamically with parameters specifying the sender, receiver, subject, and content of the email.

-   **`dags/email_workflow_dag/send_email_script.py`**: This script contains the actual Python function (`send_plain_text_email`) that sends the email using `smtplib`. It connects to an SMTP server and sends a plain text email.

#### 2. SAP Data Workflow

-   **`dags/sap_workflow_dag/api_sap_workflow.py`**: This file defines the Airflow DAG (`api_sap_workflow`) for the SAP data workflow. The DAG is scheduled to run weekly but can also be triggered manually. It orchestrates the process of fetching data from an SAP endpoint, parsing it, and upserting it into a database.

-   **`dags/sap_workflow_dag/sap_workflow_logic.py`**: This module contains the business logic for the SAP workflow:
    -   `get_data_from_sap`: Fetches data from a hardcoded SAP API endpoint.
    -   `parse_sap_data`: Parses the JSON response from the SAP API to extract the relevant vendor data.
    -   `upsert_data_to_db`: Connects to a PostgreSQL or MySQL database and performs a bulk `INSERT ... ON CONFLICT` (upsert) operation to update the `user master` table with the fetched vendor data.

## How to Use

### Prerequisites

1.  **Airflow**: A running Airflow instance is required. Ensure that the Airflow web server and scheduler are active.
2.  **Python Dependencies**: Install the required Python packages:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Environment Variables**: Create a `.env` file in the root directory of the project by copying `.env_example` and fill in the required values for your Airflow and database connections, and email credentials.

### Setup

1.  **Move DAGs to Airflow**: Airflow needs to be able to discover the DAGs. You must move or symlink the DAG definition files into your Airflow DAGs folder. Assuming your Airflow DAGs folder is at `~/airflow/dags`:

    ```bash
    # Navigate to your project directory
    cd /path/to/your/airflow_workflow_automation_app

    # Create the target directories in the Airflow dags folder
    mkdir -p ~/airflow/dags/email_workflow_dag
    mkdir -p ~/airflow/dags/sap_workflow_dag

    # Copy the DAGs and their dependencies
    cp dags/email_workflow_dag/* ~/airflow/dags/email_workflow_dag/
    cp dags/sap_workflow_dag/* ~/airflow/dags/sap_workflow_dag/
    ```

2.  **Run the Flask Application**:

    ```bash
    python flask_app.py
    ```

    The application will start on `http://127.0.0.1:5000` by default.

### Triggering Workflows

You can trigger the workflows by sending POST requests to the Flask application's endpoints.

-   **Triggering the Email Workflow**:

    ```bash
    curl -X POST http://127.0.0.1:5000/execute_mail_workflow \
    -H "Content-Type: application/json" \
    -d '{
      "sender_email": "your_email@example.com",
      "receiver_email": "recipient@example.com",
      "email_subject": "Test from Airflow",
      "email_content": "This is a test email.",
      "app_password": "your_app_password"
    }'
    ```

-   **Triggering the SAP Database Workflow**:

    ```bash
    curl -X POST http://127.0.0.1:5000/execute_database_workflow \
    -H "Content-Type: application/json" \
    -d '{}' # This workflow does not require a request body
    ```

## Important Notes

-   **Workflow Logic**: The logic of both the email and SAP workflows has been tested, and both should work seamlessly when executed as Airflow DAGs.
-   **Authentication**: Authentication credentials (like API keys and passwords) are currently managed through environment variables. Authentication flow to register new users and store their credentials is not implemented yet. However, you can populate the auth file with creds and that should work. 
