# mlops_cli/databricks_client.py
import os
import requests

class DatabricksAPI:

    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        if not self.host or not self.token:
            raise ValueError("Databricks host and token must be provided.")
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.api_version = "2.1"

    def _call_api(self, method: str, endpoint: str, payload=None):
        
        url = f"{self.host}/api/{self.api_version}/{endpoint}"
        response = requests.request(method, url, headers=self.headers, json=payload)
        if not response.ok:
            raise Exception(f"Databricks API error {response.status_code}: {response.text}")
        return response.json() if response.text else {}

    def create_job(self, payload: dict):
        """Create a new job."""
        return self._call_api("POST", "jobs/create", payload)

    def list_jobs(self):
        """List all jobs."""
        return self._call_api("GET", "jobs/list")

    def delete_job(self, job_id: str):
        """Delete a job by ID."""
        return self._call_api("POST", "jobs/delete", {"job_id": job_id})
