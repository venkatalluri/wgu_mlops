# Databricks notebook source
# ============================================================
# JOB 2: INFERENCE NOTEBOOK
# - Loads registered model from MLflow Model Registry
# - Loads input data (could be from CSV, DBFS, or sklearn dataset)
# - Runs inference
# - Saves results with timestamp
# ============================================================

# COMMAND ----------
import pandas as pd
import numpy as np
from sklearn.datasets import load_iris
import mlflow
import mlflow.sklearn
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from mlflow import MlflowClient
import os


datetime.now(timezone.utc)
spark = SparkSession.builder.getOrCreate()


# ------------------------------------------------------------
# 1 Load data for inference
# ------------------------------------------------------------
iris = load_iris(as_frame=True)
df = iris.frame.copy()
df.columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "target"]

# Create engineered features (same as in training)
df["petal_area"] = df["petal_length"] * df["petal_width"]
df["sepal_area"] = df["sepal_length"] * df["sepal_width"]

# Drop target to simulate unlabeled data
X_new = df.drop(columns=["target"])

display(X_new.head())

# COMMAND ----------
# ------------------------------------------------------------
# 2️⃣ Load model by alias
# ------------------------------------------------------------
CATALOG = "main"
SCHEMA = "default"
MODEL_NAME = "IrisClassifier"
ALIAS = "production"
client = MlflowClient()
# Get information about the model
model_info = client.get_model_version_by_alias(MODEL_NAME, ALIAS)
model_tags = model_info.tags
print(model_tags)
# model_uri = f"models:/{CATALOG}/{SCHEMA}/{MODEL_NAME}@{ALIAS}"
model_uri = f"models:/{MODEL_NAME}@{ALIAS}"
#model = mlflow.pyfunc.load_model(model_uri)
model = mlflow.sklearn.load_model(model_uri)
print(f"✅ Loaded model from MLflow registry: {model_uri}")


print(f"✅ Loaded model '{MODEL_NAME}' from MLflow registry.")

# COMMAND ----------
# ------------------------------------------------------------
# 3️⃣ Perform inference
# ------------------------------------------------------------
preds = model.predict(X_new)
results_df = X_new.copy()
results_df["prediction"] = preds
results_df["inference_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

display(results_df.head())

# COMMAND ----------
# ------------------------------------------------------------
# 4️⃣ Save inference results
# ------------------------------------------------------------

# Option 1: Save to Local CSV
timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
output_dir = f"/Workspace/Users/{os.getenv('USER', 'alluri.venkat1988@gmail.com')}/tmp"
os.makedirs(output_dir, exist_ok=True)
output_path = f"{output_dir}/inference_results_{timestamp}.csv"

results_df.to_csv(output_path, index=False)
print(f"✅ Inference results saved to workspace path: {output_path}")

# Option 2 (optional): Save to Delta table for history
results_df["inference_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
spark_df = spark.createDataFrame(results_df)
spark_df.write.mode("append").format("delta").saveAsTable("mlops_inference_results")

print("✅ Inference results appended to Delta table: mlops_inference_results")
