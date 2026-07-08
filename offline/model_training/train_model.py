#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Entrena y valida un modelo binario para la práctica Flink/Kafka/A-B testing.

Uso:
    python model_training/train_model.py \
        --x data/X_train.csv \
        --y data/y_train.csv \
        --out models \
        --reports reports \
        --threshold-min-precision 0.80

El script:
  1. Comprueba alineación por idx entre X_train e y_train.
  2. Entrena baseline con age/man/woman.
  3. Entrena modelo completo con demografía + histórico.
  4. Analiza umbrales.
  5. Entrena modelo final con todos los datos.
  6. Exporta PMML sin depender de sklearn2pmml.
"""

import argparse
import json
import os
import html
import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--x", required=True, help="Ruta a X_train.csv")
    parser.add_argument("--y", required=True, help="Ruta a y_train.csv")
    parser.add_argument("--out", default="models", help="Directorio de salida para modelo")
    parser.add_argument("--reports", default="reports", help="Directorio de salida para métricas")
    parser.add_argument("--target", default="label", help="Columna objetivo en y_train")
    parser.add_argument("--id-column", default="idx", help="Columna identificadora común")
    parser.add_argument("--threshold-min-precision", type=float, default=0.80,
                        help="Precisión mínima para seleccionar umbral conservador")
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def write_logistic_regression_pmml(path, model, feature_names, target_name="label",
                                   model_name="SilocomproLogisticRegression"):
    """
    Exportador PMML mínimo para regresión logística binaria sin preprocesado.

    El PMML contiene:
      - DataDictionary
      - RegressionModel classification normalizationMethod='logit'
      - OutputField probability_1
    """
    coef = model.coef_[0]
    intercept = float(model.intercept_[0])
    n_features = len(feature_names)

    def esc(value):
        return html.escape(str(value), quote=True)

    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<PMML version="4.4" xmlns="http://www.dmg.org/PMML-4_4" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">')
    lines.append('  <Header copyright="UIMP Big Data practice" description="Binary logistic regression model for silocompro.com A/B testing">')
    lines.append('    <Application name="scikit-learn" version="generated"/>')
    lines.append('  </Header>')
    lines.append(f'  <DataDictionary numberOfFields="{n_features + 1}">')
    for name in feature_names:
        if name == "age":
            lines.append(f'    <DataField name="{esc(name)}" optype="continuous" dataType="double"/>')
        else:
            lines.append(f'    <DataField name="{esc(name)}" optype="continuous" dataType="integer"/>')
    lines.append(f'    <DataField name="{esc(target_name)}" optype="categorical" dataType="integer">')
    lines.append('      <Value value="0"/>')
    lines.append('      <Value value="1"/>')
    lines.append('    </DataField>')
    lines.append('  </DataDictionary>')
    lines.append(f'  <RegressionModel modelName="{esc(model_name)}" functionName="classification" algorithmName="logisticRegression" normalizationMethod="logit" targetFieldName="{esc(target_name)}">')
    lines.append('    <MiningSchema>')
    for name in feature_names:
        lines.append(f'      <MiningField name="{esc(name)}" usageType="active" invalidValueTreatment="asMissing"/>')
    lines.append(f'      <MiningField name="{esc(target_name)}" usageType="target"/>')
    lines.append('    </MiningSchema>')
    lines.append('    <Output>')
    lines.append('      <OutputField name="probability_1" optype="continuous" dataType="double" feature="probability" value="1"/>')
    lines.append('      <OutputField name="predicted_label" optype="categorical" dataType="integer" feature="predictedValue"/>')
    lines.append('    </Output>')
    lines.append(f'    <RegressionTable intercept="{intercept:.17g}" targetCategory="1">')
    for name, coefficient in zip(feature_names, coef):
        lines.append(f'      <NumericPredictor name="{esc(name)}" coefficient="{float(coefficient):.17g}"/>')
    lines.append('    </RegressionTable>')
    lines.append('  </RegressionModel>')
    lines.append('</PMML>')

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def fit_logistic(X_train, y_train, features):
    model = LogisticRegression(max_iter=1000, solver="lbfgs")
    model.fit(X_train[features], y_train)
    return model


def evaluate(model, X_val, y_val, features, threshold=0.5):
    proba = model.predict_proba(X_val[features])[:, 1]
    pred = (proba >= threshold).astype(int)

    return {
        "auc": float(roc_auc_score(y_val, proba)),
        "accuracy": float(accuracy_score(y_val, pred)),
        "f1": float(f1_score(y_val, pred)),
        "precision": float(precision_score(y_val, pred, zero_division=0)),
        "recall": float(recall_score(y_val, pred, zero_division=0)),
        "shown_rate": float(pred.mean()),
    }, proba


def main():
    args = parse_args()

    os.makedirs(args.out, exist_ok=True)
    os.makedirs(args.reports, exist_ok=True)

    X = pd.read_csv(args.x)
    y = pd.read_csv(args.y)

    if args.id_column not in X.columns or args.id_column not in y.columns:
        raise ValueError(f"No existe la columna común {args.id_column!r}")

    if not np.array_equal(X[args.id_column].to_numpy(), y[args.id_column].to_numpy()):
        raise ValueError("X_train e y_train no están alineados por idx")

    if args.target not in y.columns:
        raise ValueError(f"No existe la columna objetivo {args.target!r} en y_train")

    if X.isna().any().any() or y.isna().any().any():
        raise ValueError("Hay valores perdidos. Revísalos antes de entrenar.")

    features = [c for c in X.columns if c != args.id_column]
    baseline_features = ["age", "man", "woman"]

    X_train, X_val, y_train, y_val = train_test_split(
        X[features],
        y[args.target],
        test_size=0.2,
        random_state=args.random_state,
        stratify=y[args.target],
    )

    baseline = fit_logistic(X_train, y_train, baseline_features)
    full = fit_logistic(X_train, y_train, features)

    baseline_metrics, _ = evaluate(baseline, X_val, y_val, baseline_features, threshold=0.5)
    full_metrics, full_proba = evaluate(full, X_val, y_val, features, threshold=0.5)

    rows = []
    for threshold in np.arange(0.30, 0.91, 0.05):
        pred = (full_proba >= threshold).astype(int)
        rows.append({
            "threshold": round(float(threshold), 2),
            "shown_rate": float(pred.mean()),
            "precision": float(precision_score(y_val, pred, zero_division=0)),
            "recall": float(recall_score(y_val, pred, zero_division=0)),
            "f1": float(f1_score(y_val, pred, zero_division=0)),
            "accuracy": float(accuracy_score(y_val, pred)),
            "tp": int(((pred == 1) & (y_val.to_numpy() == 1)).sum()),
            "fp": int(((pred == 1) & (y_val.to_numpy() == 0)).sum()),
            "fn": int(((pred == 0) & (y_val.to_numpy() == 1)).sum()),
            "tn": int(((pred == 0) & (y_val.to_numpy() == 0)).sum()),
        })

    threshold_df = pd.DataFrame(rows)
    threshold_df.to_csv(os.path.join(args.reports, "threshold_analysis.csv"), index=False)

    eligible = threshold_df[threshold_df["precision"] >= args.threshold_min_precision]
    if len(eligible) > 0:
        selected = eligible.loc[eligible["f1"].idxmax()].to_dict()
    else:
        selected = threshold_df.loc[threshold_df["f1"].idxmax()].to_dict()

    # Reentrenamos el modelo final con todos los datos disponibles.
    final_model = LogisticRegression(max_iter=1000, solver="lbfgs")
    final_model.fit(X[features], y[args.target])

    pmml_path = os.path.join(args.out, "model.pmml")
    write_logistic_regression_pmml(pmml_path, final_model, features, target_name=args.target)

    with open(os.path.join(args.out, "feature_order.json"), "w", encoding="utf-8") as f:
        json.dump(features, f, indent=2)

    metadata = {
        "dataset": {
            "n_rows": int(X.shape[0]),
            "n_features": int(len(features)),
            "positive_rate": float(y[args.target].mean()),
            "target": args.target,
            "id_column": args.id_column,
        },
        "split": {
            "test_size": 0.2,
            "random_state": args.random_state,
            "stratified": True,
        },
        "baseline_features": baseline_features,
        "model_features": features,
        "baseline_metrics_at_0_5": baseline_metrics,
        "full_model_metrics_at_0_5": full_metrics,
        "selected_threshold": float(selected["threshold"]),
        "selected_threshold_metrics": selected,
        "model": {
            "type": "LogisticRegression",
            "solver": "lbfgs",
            "max_iter": 1000,
            "preprocessing": "none",
            "pmml_file": pmml_path,
        },
    }

    with open(os.path.join(args.reports, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print("Entrenamiento finalizado.")
    print(f"Baseline AUC: {baseline_metrics['auc']:.4f}")
    print(f"Modelo completo AUC: {full_metrics['auc']:.4f}")
    print(f"Umbral seleccionado: {float(selected['threshold']):.2f}")
    print(f"PMML: {pmml_path}")


if __name__ == "__main__":
    main()
