# mlops_cli/cli.py
import argparse
from mlops_cli.DataBricksAPI import DatabricksAPI
from mlops_cli.MLJobs import MLJobs

def main():
    parser = argparse.ArgumentParser(description="MLOps CLI for Databricks automation")
    parser.add_argument("command", choices=["create-jobs", "list-jobs", "delete-job"],
                        help="Action to perform")
    parser.add_argument("--job-id", help="Job ID to delete (required for delete-job)")
    args = parser.parse_args()

    # Initialize objects
    client = DatabricksAPI()
    job_creator = MLJobs(client)

    if args.command == "create-jobs":
        job_creator.create_training_job()
        job_creator.create_inference_job()
    elif args.command == "list-jobs":
        jobs = client.list_jobs()
        print(jobs)
    elif args.command == "delete-job":
        if not args.job_id:
            print("Please provide --job-id")
        else:
            client.delete_job(args.job_id)
            print(f"Job {args.job_id} deleted successfully.")

if __name__ == "__main__":
    main()
