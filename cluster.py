#!/usr/bin/env python3
"""ME-OPS Behavioral Clustering — discover your work modes.

Extracts session feature vectors, reduces dimensionality with UMAP,
and clusters with HDBSCAN to identify distinct behavioral modes
(e.g., deep coding, research, context thrashing).

Skills used: ai-engineer (embedding + clustering pipeline),
             software-architecture (feature engineering)

Ref: https://hdbscan.readthedocs.io (official HDBSCAN docs)
     https://umap-learn.readthedocs.io (official UMAP docs)
     https://scikit-learn.org/stable/modules/preprocessing.html

Usage:
    python cluster.py              # Run full pipeline
    python cluster.py --refresh    # Rebuild features + recluster
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
from typing import Optional

DB_PATH = Path(__file__).parent / "me_ops.duckdb"


# ---------------------------------------------------------------------------
# 1. Feature engineering — extract session feature vectors
# ---------------------------------------------------------------------------

def extract_session_features(con: duckdb.DuckDBPyConnection) -> tuple[np.ndarray, list[int]]:
    """Build feature matrix from sessions table.

    Features per session:
    - duration_min
    - event_count
    - unique_actions
    - hour_of_day (cyclical: sin + cos)
    - day_of_week (cyclical: sin + cos)
    - project_count (from comma-separated projects)
    - is_late_night (after 11 PM or before 5 AM)
    """
    df = con.execute("""
        SELECT
            session_id,
            duration_min,
            event_count,
            unique_actions,
            EXTRACT(HOUR FROM ts_start) AS hour_of_day,
            EXTRACT(DOW FROM ts_start) AS day_of_week,
            CASE WHEN projects IS NOT NULL AND projects != ''
                 THEN LENGTH(projects) - LENGTH(REPLACE(projects, ',', '')) + 1
                 ELSE 0 END AS project_count,
            projects
        FROM sessions
        WHERE duration_min > 0
        ORDER BY session_id
    """).fetchdf()

    if df.empty:
        return np.array([]), []


    # Cyclical encoding for time features
    df["hour_sin"] = np.sin(2 * np.pi * df["hour_of_day"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour_of_day"] / 24)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
    df["is_late_night"] = ((df["hour_of_day"] >= 23) | (df["hour_of_day"] < 5)).astype(int)

    feature_cols = [
        "duration_min", "event_count", "unique_actions",
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
        "project_count", "is_late_night",
    ]

    X = df[feature_cols].values.astype(np.float64)
    session_ids = df["session_id"].tolist()

    # Replace NaN with 0
    X = np.nan_to_num(X, nan=0.0)

    return X, session_ids


# ---------------------------------------------------------------------------
# 2. Dimensionality reduction + clustering
# ---------------------------------------------------------------------------

def cluster_sessions(
    X: np.ndarray,
    session_ids: list[int],
    min_cluster_size: int = 5,
    n_components: int = 3,
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Reduce dimensions with UMAP, cluster with HDBSCAN.

    Returns:
        embedding: UMAP-reduced coordinates (n_samples, n_components)
        labels: cluster labels (-1 = noise)
        cluster_names: human-readable names for each cluster
    """
    from sklearn.preprocessing import StandardScaler

    # Standardize features (critical for distance-based methods)
    # Ref: https://scikit-learn.org/stable/modules/preprocessing.html#standardization
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # UMAP dimensionality reduction
    # Ref: https://umap-learn.readthedocs.io/en/latest/parameters.html
    try:
        import umap

        reducer = umap.UMAP(
            n_components=n_components,
            n_neighbors=min(15, len(X_scaled) - 1),
            min_dist=0.1,
            metric="euclidean",
            random_state=42,
        )
        embedding = reducer.fit_transform(X_scaled)
    except ImportError:
        print("  WARNING: umap-learn not installed. Using PCA fallback.")
        from sklearn.decomposition import PCA

        reducer = PCA(n_components=min(n_components, X_scaled.shape[1]))
        embedding = reducer.fit_transform(X_scaled)

    # HDBSCAN clustering
    # Ref: https://hdbscan.readthedocs.io/en/latest/parameter_selection.html
    try:
        import hdbscan

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            min_samples=3,
            cluster_selection_epsilon=0.5,
        )
        labels = clusterer.fit_predict(embedding)
    except ImportError:
        print("  WARNING: hdbscan not installed. Using KMeans fallback (k=4).")
        from sklearn.cluster import KMeans

        km = KMeans(n_clusters=min(4, len(X_scaled)), random_state=42, n_init="auto")
        labels = km.fit_predict(embedding)

    # Generate cluster names based on centroid features
    cluster_names = _name_clusters(X, labels)

    return np.asarray(embedding), np.asarray(labels), cluster_names


def _name_clusters(X: np.ndarray, labels: np.ndarray) -> list[str]:
    """Assign human-readable names to clusters based on feature centroids."""
    feature_names = [
        "duration", "events", "actions",
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
        "projects", "late_night",
    ]
    unique_labels = sorted(set(labels))
    names = []

    for label in unique_labels:
        if label == -1:
            names.append("Noise/Outlier")
            continue

        mask = labels == label
        centroid = X[mask].mean(axis=0)

        # Decision tree for naming
        duration = centroid[0]
        events = centroid[1]
        late_night = centroid[8]
        projects = centroid[7]

        if late_night > 0.5:
            name = "Late Night Push"
        elif duration > 60 and events > 50:
            name = "Deep Work Session"
        elif projects >= 3:
            name = "Context Thrashing"
        elif duration < 10:
            name = "Quick Check-in"
        elif events > 30:
            name = "Active Coding"
        else:
            name = f"Mode-{label}"

        names.append(name)

    return names


# ---------------------------------------------------------------------------
# 3. Persist results + report
# ---------------------------------------------------------------------------

def save_clusters(
    con: duckdb.DuckDBPyConnection,
    session_ids: list[int],
    labels: np.ndarray,
    cluster_names: list[str],
) -> None:
    """Save cluster assignments to DB."""
    con.execute("DROP TABLE IF EXISTS session_clusters")
    con.execute("""
        CREATE TABLE session_clusters (
            session_id INTEGER,
            cluster_id INTEGER,
            cluster_name VARCHAR
        )
    """)

    unique_labels = sorted(set(labels))
    label_to_name = dict(zip(unique_labels, cluster_names))

    rows = [
        (sid, int(label), label_to_name.get(label, "Unknown"))
        for sid, label in zip(session_ids, labels)
    ]

    if rows:
        placeholders = ",".join(["(?,?,?)"] * len(rows))
        flat = [v for row in rows for v in row]
        con.execute(f"INSERT INTO session_clusters VALUES {placeholders}", flat)

    print(f"  Saved {len(rows)} cluster assignments")


def print_report(
    X: np.ndarray,
    session_ids: list[int],
    labels: np.ndarray,
    cluster_names: list[str],
) -> None:
    """Print cluster summary report."""
    unique_labels = sorted(set(labels))

    print()
    print("=" * 60)
    print("  BEHAVIORAL CLUSTER REPORT")
    print("=" * 60)

    for i, label in enumerate(unique_labels):
        mask = labels == label
        count = mask.sum()
        centroid = X[mask].mean(axis=0)
        name = cluster_names[i]

        print(f"\n  Cluster {label}: {name} ({count} sessions)")
        print(f"    Avg duration:   {centroid[0]:.1f} min")
        print(f"    Avg events:     {centroid[1]:.0f}")
        print(f"    Avg actions:    {centroid[2]:.0f}")
        print(f"    Avg projects:   {centroid[7]:.1f}")
        print(f"    Late night:     {centroid[8]:.0%}")

    print()
    print(f"  Total sessions analyzed: {len(session_ids)}")
    print(f"  Clusters found: {len([l for l in unique_labels if l != -1])}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(db_path: Path, *, con: Optional[duckdb.DuckDBPyConnection] = None) -> None:
    """Run full clustering pipeline."""
    print("ME-OPS Behavioral Clustering")
    print("=" * 60)

    close_con = False
    if con is None:
        con = duckdb.connect(str(db_path))
        close_con = True

    try:
        print("  Extracting session features...")
        X, session_ids = extract_session_features(con)

        if len(X) == 0:
            print("  ERROR: No sessions found.")
            return

        print(f"  {len(session_ids)} sessions, {X.shape[1]} features")

        # Adjust min_cluster_size based on dataset size
        min_cluster_size = max(3, len(session_ids) // 20)
        print(f"  Min cluster size: {min_cluster_size}")

        print("  Running UMAP + HDBSCAN...")
        embedding, labels, cluster_names = cluster_sessions(
            X, session_ids, min_cluster_size=min_cluster_size
        )

        save_clusters(con, session_ids, labels, cluster_names)
        print_report(X, session_ids, labels, cluster_names)
    finally:
        if close_con:
            con.close()
    
    print("\n✅ Clustering complete")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="ME-OPS Behavioral Clustering")
    parser.add_argument("--db", type=str, default=str(DB_PATH), help="Path to DuckDB")
    args = parser.parse_args()

    run(Path(args.db))


if __name__ == "__main__":
    main()
