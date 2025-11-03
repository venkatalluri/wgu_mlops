# mlops_cli/job_creator.py
import os
from mlops_cli.DataBricksAPI import DatabricksAPI

class MLJobs:
    def __init__(self, client: DatabricksAPI):
        self.client = client
        self.cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")

    def _build_job_payload(self, job_name: str, notebook_path: str, schedule: str):
        """Builds the JSON payload for the job creation request."""
        return {
            "name": job_name,
            "tasks": [
                {
                    "task_key": job_name,
                    "notebook_task": {"notebook_path": notebook_path},
                    "existing_cluster_id": self.cluster_id,
                }
            ],
            "schedule": {
                "quartz_cron_expression": schedule,
                "timezone_id": "UTC",
                "pause_status": "UNPAUSED"
            }
        }

    def create_training_job(self):
        payload = self._build_job_payload(
            job_name="job_train_model",
            notebook_path="/Repos/venkat.alluri@wgu.edu/mlops-assessment/notebooks/train_model",
            schedule="0 0 1 * *"  # every 30 days
        )
        response = self.client.create_job(payload)
        print(f" Training job created: {response.get('job_id', 'N/A')}")

    def create_inference_job(self):
        payload = self._build_job_payload(
            job_name="job_inference_model",
            notebook_path="/Repos/venkat.alluri@wgu.edu/mlops-assessment/notebooks/run_inference",
            schedule="0 0 * * *"  # daily
        )
        response = self.client.create_job(payload)
        print(f"Inference job created: {response.get('job_id', 'N/A')}")
