# mlops_cli/databricks_client.py
import os
import requests
import json

class DatabricksAPI:

    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.token = os.getenv("DATABRICKS_TOKEN")
        if not self.host or not self.token:
            raise ValueError("Databricks host and token must be provided.")
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.api_version = "2.1"

    def _call_api(self, method: str, endpoint: str, payload=None):
        
        url = f"{self.host}/api/{endpoint}"
        response = requests.request(method, url, headers=self.headers, json=payload)
        if not response.ok:
            raise Exception(f"Databricks API error {response.status_code}: {response.text}")
        result= response.json() if response.text else {}
        return result

    def create_job(self, payload: dict):
        """Create a new job."""
        return self._call_api("POST", "/2.2/jobs/create", payload)

    def list_jobs(self):
        """List all jobs."""
        return self._call_api("GET", "/2.2/jobs/list")

    def delete_job(self, job_id: str):
        """Delete a job by ID."""
        return self._call_api("POST", "/2.2/jobs/delete", {"job_id": job_id})

    def get_repo_id(self, repo_name: str):
        """Get the repository ID by name."""
        repos = self._call_api("GET", "/2.0/repos?path_prefix=/Workspace/Repos/wgu_mlops")['repos']
        for repo in repos:
            if repo['path'].endswith(repo_name):
                return repo['id']
        raise ValueError(f"Repository '{repo_name}' not found.")
    
    def update_repo(self, repo_id: str, branch: str = "main"):
        """Update the repository to the latest commit on the specified branch."""
        payload = {
            "repo_id": repo_id,
            "branch": branch
        }
        return self._call_api("PATCH", f"/2.0/repos/{repo_id}", payload)
    
    def update_job(self, job_id: str, payload: dict):
        """Update an existing job."""
        payload["job_id"] = job_id
        return self._call_api("POST", "/2.2/jobs/reset", payload)

