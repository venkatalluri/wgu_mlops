# Databricks notebook source
# ============================================================
# JOB 1: TRAINING NOTEBOOK
# - Loads the Iris dataset
# - Performs simple feature engineering
# - Trains a Logistic Regression classifier
# - Logs params & metrics with MLflow
# - Registers model to MLflow Model Registry
# ============================================================

# COMMAND ----------
# Imports
import pandas as pd
import numpy as np
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

# COMMAND ----------
# MLflow setup
mlflow.set_experiment("/Shared/iris_experiment")

with mlflow.start_run(run_name="iris_logreg_train"):
    
    iris = load_iris(as_frame=True)
    df = iris.frame
    df.columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "target"]
    print("✅ Loaded dataset shape:", df.shape)
    display(df.head())


    df["petal_area"] = df["petal_length"] * df["petal_width"]
    df["sepal_area"] = df["sepal_length"] * df["sepal_width"]

    features = ["sepal_length", "sepal_width", "petal_length", "petal_width",
                "petal_area", "sepal_area"]
    X = df[features]
    y = df["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )


    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    C = 1.0  # regularization strength
    model = LogisticRegression(max_iter=200, C=C)
    model.fit(X_train_scaled, y_train)


    preds = model.predict(X_test_scaled)
    acc = accuracy_score(y_test, preds)
    prec = precision_score(y_test, preds, average="macro")
    rec = recall_score(y_test, preds, average="macro")
    f1 = f1_score(y_test, preds, average="macro")

    print(f"✅ Accuracy: {acc:.3f}, Precision: {prec:.3f}, Recall: {rec:.3f}, F1: {f1:.3f}")

    signature = infer_signature(X_train_scaled, model.predict(X_train_scaled))
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("regularization_strength", C)
    mlflow.log_metric("accuracy", acc)
    mlflow.log_metric("precision", prec)
    mlflow.log_metric("recall", rec)
    mlflow.log_metric("f1_score", f1)


    mlflow.sklearn.log_model(model, artifact_path="model", signature=signature)
    model_uri = f"runs:/{mlflow.active_run().info.run_id}/model"
    registered_model = mlflow.register_model(model_uri, "IrisClassifier")

    print(f"✅ Model registered: {registered_model.name}")

# COMMAND ----------
# Optional visualization / validation
import pandas as pd
metrics_df = pd.DataFrame([{"accuracy": acc, "precision": prec, "recall": rec, "f1": f1}])
display(metrics_df)
