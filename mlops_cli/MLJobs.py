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


    def _find_job_id(self, job_name: str):
        """Deletes an existing Databricks job if it matches the given name."""
        jobs = self.client.list_jobs()
        jobs = jobs['jobs']
        for job in jobs:
            if job.get("settings", {}).get("name") == job_name:
                print("**********************************")
                print(f" Found existing job '{job_name}' with ID: {job['job_id']}")
                print("**********************************")
                return job["job_id"]
        return None

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
    
    def _create_or_update_job(self, job_name: str, payload: dict):
        job_id = self._find_job_id(job_name)
        if job_id:
            print(f"🔄 Updating existing job '{job_name}' (ID: {job_id}) ...")
            response = self.client.update_job(job_id, payload)
            print(f"✅ Job '{job_name}' updated successfully (ID: {job_id})")
            return job_id
        else:
            print(f"🆕 Creating new job '{job_name}' ...")
            response = self.client.create_job(payload)
            new_id = response.get("job_id")
            print(f"✅ Job '{job_name}' created (ID: {new_id})")
            return new_id

    def update_repository(self):
        repo_id = self.client.get_repo_id(self.repo_name)
        if repo_id:
            print(f"🔄 Updating repository '{self.repo_name}' (ID: {repo_id}) ...")
            response = self.client.update_repo(repo_id, branch="main")
            print(f"✅ Repository '{self.repo_name}' updated successfully.")
        else:
            print(f"❌ Repository '{self.repo_name}' not found.")
    
    def create_training_job(self):
        job_name="job_train_model"
        payload = self._build_job_payload(
            job_name=job_name,
            notebook_path="/Workspace/Repos/wgu_mlops/wgu_mlops/mlops_engg_nb/model_traning",
            schedule="0 0 0 1 * ?"  # every 30 days
        )
        job_id=self._create_or_update_job(job_name, payload)
        print(f" Training job created: {job_id}")

    def create_inference_job(self):
        job_name="job_inference_model"
        payload = self._build_job_payload(
            job_name="job_inference_model",
            notebook_path="/Workspace/Repos/wgu_mlops/wgu_mlops/mlops_engg_nb/model_inference",
            schedule="0 0 0 * * ?"  # daily
        )
        job_id=self._create_or_update_job(job_name, payload)
        print(f" Inference job created: {job_id}")
