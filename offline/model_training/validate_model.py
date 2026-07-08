#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Muestra las métricas generadas por train_model.py de forma legible.
"""

import argparse
import json
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", default="reports/metrics.json")
    parser.add_argument("--thresholds", default="reports/threshold_analysis.csv")
    args = parser.parse_args()

    with open(args.metrics, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    print("\n=== Dataset ===")
    print(f"Filas: {metrics['dataset']['n_rows']}")
    print(f"Features: {metrics['dataset']['n_features']}")
    print(f"Tasa positivos: {metrics['dataset']['positive_rate']:.4f}")

    print("\n=== Baseline edad + sexo, umbral 0.5 ===")
    for key, value in metrics["baseline_metrics_at_0_5"].items():
        print(f"{key}: {value:.4f}")

    print("\n=== Modelo completo, umbral 0.5 ===")
    for key, value in metrics["full_model_metrics_at_0_5"].items():
        print(f"{key}: {value:.4f}")

    print("\n=== Umbral recomendado ===")
    print(f"threshold: {metrics['selected_threshold']:.2f}")
    for key, value in metrics["selected_threshold_metrics"].items():
        if key != "threshold":
            print(f"{key}: {value:.4f}" if isinstance(value, float) else f"{key}: {value}")

    print("\n=== Tabla de umbrales ===")
    df = pd.read_csv(args.thresholds)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
