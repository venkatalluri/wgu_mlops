# Databricks notebook source
# ============================================================
# INFERENCE NOTEBOOK – Loads latest model from MLflow Registry
# Runs prediction on sample data and saves results
# ============================================================

import mlflow
import pandas as pd
from datetime import datetime

# ------------------------------------------------------------
# 1️⃣  Load latest production model
# ------------------------------------------------------------
model_name = "IrisClassifier"

try:
    model = mlflow.sklearn.load_model(f"models:/{model_name}/Latest")
    print(f"✅ Loaded model: {model_name}")
except Exception as e:
    print(f"❌ Failed to load model '{model_name}'. Error: {e}")
    raise

# ------------------------------------------------------------
# 2️⃣  Prepare new data for prediction
# ------------------------------------------------------------
sample_data = pd.DataFrame(
    [
        [5.1, 3.5, 1.4, 0.2],
        [6.7, 3.1, 4.4, 1.4],
        [7.2, 3.6, 6.1, 2.5],
    ],
    columns=["f1", "f2", "f3", "f4"],
)

# ------------------------------------------------------------
# 3️⃣  Run inference
# ------------------------------------------------------------
preds = model.predict(sample_data)
sample_data["prediction"] = preds

print("✅ Inference complete. Predictions:")
print(sample_data)

# ------------------------------------------------------------
# 4️⃣  Save results to DBFS (Databricks File System)
# ------------------------------------------------------------
output_path = f"/dbfs/tmp/inference_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
sample_data.to_csv(output_path, index=False)

print(f"✅ Predictions saved to: {output_path}")
