# mlops_cli/job_creator.py
import os
from mlops_cli.DataBricksAPI import DatabricksAPI

class MLJobs:
    def __init__(self, client: DatabricksAPI):
        self.client = client
        self.user = os.getenv("DATABRICKS_USER", "alluri.venkat1988@gmail.com")
        self.repo_name = os.getenv("DATABRICKS_REPO_NAME", "wgu_mlops")

        # Base repo path for notebooks
        self.base_path = f"/Users/{self.user}/{self.repo_name}/mlops_engg"


    def _delete_job_if_exists(self, job_name: str):
        """Deletes an existing Databricks job if it matches the given name."""
        jobs = self.client.list_jobs()
        jobs = jobs['jobs']
        print("**********************************")
        for job in jobs:
            print(job)
            print("**********************************")
            if job.get("settings", {}).get("name") == job_name:
                job_id = job["job_id"]
                print(f"Found existing job '{job_name}' (ID: {job_id}), deleting...")
                self.client.delete_job(job_id)
                print(f"Deleted job '{job_name}' (ID: {job_id})")
                return True
        print(f"No existing job found for '{job_name}'.")
        return False

    def _build_job_payload(self, job_name: str, notebook_path: str, schedule: str):
        return {
            "name": job_name,
            "tasks": [
                {
                    "task_key": f"{job_name}_task",
                    "notebook_task": {
                        "notebook_path": notebook_path
                    },
                    "compute": {"compute_type": "SERVERLESS"},
                }
            ],
            "schedule": {
                "quartz_cron_expression": schedule,
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED"
            }
        }

    def create_training_job(self):
        job_name="job_train_model"
        self._delete_job_if_exists(job_name)
        payload = self._build_job_payload(
            job_name=job_name,
            notebook_path="/Workspace/Users/alluri.venkat1988@gmail.com/wgu_mlops/mlops_engg_nb/model_traning",
            schedule="0 0 0 1 * ?"  # every 30 days
        )
        response = self.client.create_job(payload)
        print(f" Training job created: {response.get('job_id', 'N/A')}")

    def create_inference_job(self):
        job_name="job_inference_model"
        self._delete_job_if_exists(job_name)
        payload = self._build_job_payload(
            job_name="job_inference_model",
            notebook_path="/Workspace/Users/alluri.venkat1988@gmail.com/wgu_mlops/mlops_engg_nb/model_inference",
            schedule="0 0 0 * * ?"  # daily
        )
        response = self.client.create_job(payload)
        print(f"Inference job created: {response.get('job_id', 'N/A')}")
