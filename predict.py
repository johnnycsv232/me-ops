#!/usr/bin/env python3
"""ME-OPS Predictive Modeling — forecast behavioral patterns.

Trains models on session features to predict:
1. Will I context-thrash today? (classification)
2. How long will my next session be? (regression)
3. What project will I focus on? (multi-class)

Skills used: ai-engineer (ML pipeline),
             testing-patterns (train/test split, cross-validation)

Ref: https://scikit-learn.org/stable/modules/ensemble.html#random-forests
     https://scikit-learn.org/stable/modules/cross_validation.html

Usage:
    python predict.py              # Train + evaluate all models
    python predict.py --predict    # Predict for current session
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
from typing import Optional

DB_PATH = Path(__file__).parent / "me_ops.duckdb"


# ---------------------------------------------------------------------------
# 1. Feature engineering
# ---------------------------------------------------------------------------

def build_prediction_features(con: duckdb.DuckDBPyConnection) -> dict:
    """Build feature matrices for all prediction tasks.

    Uses lag features (previous session) and temporal features.
    Ref: https://scikit-learn.org/stable/modules/preprocessing.html
    """
    df = con.execute("""
        SELECT
            session_id,
            duration_min,
            event_count,
            unique_actions,
            EXTRACT(HOUR FROM ts_start) AS hour,
            EXTRACT(DOW FROM ts_start) AS dow,
            CASE WHEN projects IS NOT NULL AND projects != ''
                 THEN LENGTH(projects) - LENGTH(REPLACE(projects, ',', '')) + 1
                 ELSE 0 END AS project_count,
            dominant_action,
            projects,
            EXTRACT(EPOCH FROM ts_start) AS ts_epoch
        FROM sessions
        WHERE duration_min > 0
        ORDER BY ts_start
    """).fetchdf()


    if len(df) < 10:
        print("  WARNING: Not enough sessions for training (<10)")
        return {}

    # Cyclical time features
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"] = np.sin(2 * np.pi * df["dow"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["dow"] / 7)

    # Lag features (previous session context)
    df["prev_duration"] = df["duration_min"].shift(1).fillna(0)
    df["prev_events"] = df["event_count"].shift(1).fillna(0)
    df["prev_projects"] = df["project_count"].shift(1).fillna(0)
    df["time_since_prev"] = (df["ts_epoch"] - df["ts_epoch"].shift(1)).fillna(0) / 3600  # hours

    # Binary targets
    df["is_context_thrash"] = (df["project_count"] >= 3).astype(int)
    df["is_late"] = ((df["hour"] >= 23) | (df["hour"] < 5)).astype(int)

    # Feature columns (same for all models)
    feature_cols = [
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
        "prev_duration", "prev_events", "prev_projects",
        "time_since_prev",
    ]

    # Skip first row (no lag features)
    df = df.iloc[1:].reset_index(drop=True)

    X = df[feature_cols].values.astype(np.float64)
    X = np.nan_to_num(X, nan=0.0)

    return {
        "X": X,
        "feature_names": feature_cols,
        "y_duration": df["duration_min"].values,
        "y_thrash": df["is_context_thrash"].values,
        "y_late": df["is_late"].values,
        "session_ids": df["session_id"].tolist(),
        "df": df,
    }


# ---------------------------------------------------------------------------
# 2. Model training + evaluation
# ---------------------------------------------------------------------------

def train_and_evaluate(data: dict) -> dict:
    """Train Random Forest models for each prediction task.

    Uses cross-validation per sklearn best practices:
    Ref: https://scikit-learn.org/stable/modules/cross_validation.html
    """
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.model_selection import cross_val_score, TimeSeriesSplit
    from sklearn.metrics import make_scorer, f1_score

    X = data["X"]
    results: dict = {}

    # Time-series cross-validation (prevents data leakage)
    # Ref: https://scikit-learn.org/stable/modules/cross_validation.html#time-series-split
    tscv = TimeSeriesSplit(n_splits=min(5, len(X) // 10))

    # F1 scorer that handles folds with no positive class
    # Ref: https://scikit-learn.org/stable/modules/generated/sklearn.metrics.f1_score.html
    f1_scorer = make_scorer(f1_score, zero_division=0.0)

    # --- Task 1: Context thrashing prediction ---
    print("\n  📊 Task 1: Context Thrashing Prediction")
    y_thrash = data["y_thrash"]

    clf_thrash = RandomForestClassifier(
        n_estimators=100,
        max_depth=6,
        random_state=42,
        class_weight="balanced",  # handle imbalanced classes
    )

    scores = cross_val_score(clf_thrash, X, y_thrash, cv=tscv, scoring=f1_scorer)
    clf_thrash.fit(X, y_thrash)

    results["thrash"] = {
        "cv_f1_mean": float(np.mean(scores)),
        "cv_f1_std": float(np.std(scores)),
        "feature_importance": dict(zip(
            data["feature_names"],
            clf_thrash.feature_importances_.tolist()
        )),
    }
    print(f"    CV F1: {np.mean(scores):.3f} ± {np.std(scores):.3f}")

    # --- Task 2: Session duration prediction ---
    print("\n  📊 Task 2: Session Duration Prediction")
    y_duration = data["y_duration"]

    reg_duration = RandomForestRegressor(
        n_estimators=100,
        max_depth=8,
        random_state=42,
    )

    scores = cross_val_score(reg_duration, X, y_duration, cv=tscv, scoring="r2")
    reg_duration.fit(X, y_duration)

    # Also compute MAE
    from sklearn.metrics import mean_absolute_error
    y_pred = reg_duration.predict(X)
    mae = mean_absolute_error(y_duration, y_pred)

    results["duration"] = {
        "cv_r2_mean": float(np.mean(scores)),
        "cv_r2_std": float(np.std(scores)),
        "train_mae_min": float(mae),
        "feature_importance": dict(zip(
            data["feature_names"],
            reg_duration.feature_importances_.tolist()
        )),
    }
    print(f"    CV R²: {np.mean(scores):.3f} ± {np.std(scores):.3f}")
    print(f"    Train MAE: {mae:.1f} minutes")

    # --- Task 3: Late night prediction ---
    print("\n  📊 Task 3: Late Night Prediction")
    y_late = data["y_late"]

    clf_late = RandomForestClassifier(
        n_estimators=100,
        max_depth=6,
        random_state=42,
        class_weight="balanced",
    )

    scores = cross_val_score(clf_late, X, y_late, cv=tscv, scoring=f1_scorer)
    clf_late.fit(X, y_late)

    results["late_night"] = {
        "cv_f1_mean": float(np.mean(scores)),
        "cv_f1_std": float(np.std(scores)),
        "feature_importance": dict(zip(
            data["feature_names"],
            clf_late.feature_importances_.tolist()
        )),
    }
    print(f"    CV F1: {np.mean(scores):.3f} ± {np.std(scores):.3f}")

    return results


# ---------------------------------------------------------------------------
# 3. Feature importance analysis
# ---------------------------------------------------------------------------

def print_feature_importance(results: dict) -> None:
    """Print ranked feature importance for each model."""
    print("\n" + "=" * 60)
    print("  FEATURE IMPORTANCE RANKINGS")
    print("=" * 60)

    for task, data in results.items():
        if "feature_importance" not in data:
            continue
        print(f"\n  {task.upper()}:")
        sorted_features = sorted(
            data["feature_importance"].items(),
            key=lambda x: x[1],
            reverse=True,
        )
        for name, importance in sorted_features:
            bar = "█" * int(importance * 50)
            print(f"    {name:20s} {importance:.3f} {bar}")


# ---------------------------------------------------------------------------
# 4. Save predictions
# ---------------------------------------------------------------------------

def save_predictions(
    con: duckdb.DuckDBPyConnection,
    data: dict,
    results: dict,
) -> None:
    """Save model results and metrics to DB."""
    con.execute("DROP TABLE IF EXISTS prediction_metrics")
    con.execute("""
        CREATE TABLE prediction_metrics (
            task VARCHAR,
            metric_name VARCHAR,
            metric_value DOUBLE,
            trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    rows = []
    for task, metrics in results.items():
        for key, val in metrics.items():
            if isinstance(val, (int, float)):
                rows.append((task, key, float(val)))

    if rows:
        placeholders = ",".join(["(?,?,?)"] * len(rows))
        flat = [v for row in rows for v in row]
        con.execute(
            f"INSERT INTO prediction_metrics (task, metric_name, metric_value) VALUES {placeholders}",
            flat,
        )

    print(f"\n  Saved {len(rows)} metric values")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(db_path: Path, *, con: Optional[duckdb.DuckDBPyConnection] = None) -> dict:
    """Train and evaluate prediction models."""
    print("ME-OPS Predictive Modeling")
    print("=" * 60)

    close_con = False
    if con is None:
        con = duckdb.connect(str(db_path))
        close_con = True

    try:
        print("  Building features from sessions...")
        data = build_prediction_features(con)

        if not data:
            return {}

        print(f"  {len(data['session_ids'])} sessions, {data['X'].shape[1]} features")

        results = train_and_evaluate(data)
        print_feature_importance(results)
        save_predictions(con, data, results)
    finally:
        if close_con:
            con.close()

    print("\n✅ Prediction models trained and evaluated")
    return results


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="ME-OPS Predictive Modeling")
    parser.add_argument("--db", type=str, default=str(DB_PATH), help="Path to DuckDB")
    args = parser.parse_args()

    run(Path(args.db))


if __name__ == "__main__":
    main()
