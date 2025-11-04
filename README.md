WGU MLOps Take-Home Assessment

Author: Venkat Rama Raju Alluri

📋 Overview

This repository contains my submission for the WGU MLOps take-home assessment.
The goal of this project is to demonstrate how to build, train, and deploy a machine learning model into production using Databricks, MLflow, and GitHub Actions.

🧩 Assessment Summary

The assessment required building:

A Python-based CLI tool that integrates with Databricks Jobs API

Two Databricks jobs:

Job 1 – Training Job: trains and registers a classification model every 30 days

Job 2 – Inference Job: loads the trained model, performs inference, and stores predictions daily

Integration with GitHub Actions for automation

Use of MLflow for logging, tracking, and model registry

⚙️ Deliverables
Component	Description	Link / Location
✅ GitHub Repository	Source code, notebooks, workflow, and documentation	https://github.com/venkatalluri/wgu_mlops

✅ Databricks Notebooks	Training and inference notebooks for model lifecycle	train_model & run_inference
✅ Databricks Jobs	Automated scheduled jobs created via CLI tool	job_train_model (monthly)
job_inference_model (daily)
✅ MLflow Experiment	Logs of parameters, metrics, and registered models	Experiment Link available in Databricks workspace
✅ Sample Output	Inference results saved as CSV and Delta table	Available in workspace or FileStore path
🧰 Databricks Workspace Information

Workspace URL:
🔗 https://dbc-b7954e84-86d2.cloud.databricks.com/?o=3605749912064627
