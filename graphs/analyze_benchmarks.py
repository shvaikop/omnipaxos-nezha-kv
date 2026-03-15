from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

REQUIRED_COLUMNS = {
    "client",
    "op_id",
    "req_time",
    "res_time",
    "op_type",
}


def load_client_logs(directory: Path) -> pd.DataFrame:
    csv_paths = sorted(directory.glob("client-*.csv"))
    if not csv_paths:
        raise FileNotFoundError(f"No client-*.csv files found in {directory}")

    frames: list[pd.DataFrame] = []
    for csv_path in csv_paths:
        frame = pd.read_csv(csv_path)
        missing = REQUIRED_COLUMNS.difference(frame.columns)
        if missing:
            raise ValueError(f"{csv_path} is missing columns: {sorted(missing)}")

        frame = frame.copy()
        frame["client"] = frame["client"].astype(str)

        frame["req_time"] = pd.to_numeric(frame["req_time"], errors="coerce")
        frame["res_time"] = pd.to_numeric(frame["res_time"], errors="coerce")
        frame["latency_ms"] = frame["res_time"] - frame["req_time"]
        frame["request_time"] = pd.to_datetime(frame["req_time"], unit="ms", errors="coerce")
        frame["response_time"] = pd.to_datetime(frame["res_time"], unit="ms", errors="coerce")
        frame["source_file"] = csv_path.name
        frames.append(frame)

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.dropna(subset=["request_time", "response_time", "latency_ms", "op_type", "client"])
    merged = merged.sort_values(by="request_time").reset_index(drop=True)
    start_time = merged["response_time"].min()
    merged["elapsed_s"] = (
        (merged["response_time"] - start_time).dt.total_seconds().astype(int)
    )
    return merged


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def plot_series(series: pd.Series, title: str, ylabel: str, output_path: Path) -> None:
    plt.figure(figsize=(12, 4))
    plt.plot(series.index, series.values, linewidth=1.5)
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xlabel("Time (s)")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()


def plot_latency_over_time(df: pd.DataFrame, output_dir: Path) -> list[Path]:
    grouped = df.groupby("elapsed_s")["latency_ms"]
    mean_latency = grouped.mean()
    median_latency = grouped.median()
    p95_latency = grouped.quantile(0.95)

    mean_path = output_dir / "latency_mean_per_sec.png"
    median_path = output_dir / "latency_median_per_sec.png"
    p95_path = output_dir / "latency_p95_per_sec.png"

    plot_series(mean_latency, "Mean latency per second", "Latency (ms)", mean_path)
    plot_series(median_latency, "Median latency per second", "Latency (ms)", median_path)
    plot_series(p95_latency, "P95 latency per second", "Latency (ms)", p95_path)
    return [mean_path, median_path, p95_path]



def print_summary(df: pd.DataFrame) -> None:
    rps = df.groupby("elapsed_s").size()
    print("Benchmark summary")
    print("-----------------")
    print(f"Total requests: {len(df)}")
    print(f"Time range: {df['request_time'].min()} -> {df['request_time'].max()}")
    print(f"Mean latency: {df['latency_ms'].mean():.3f} ms")
    print(f"Median latency: {df['latency_ms'].median():.3f} ms")
    print(f"P90 latency: {df['latency_ms'].quantile(0.90):.3f} ms")
    print(f"P95 latency: {df['latency_ms'].quantile(0.95):.3f} ms")
    print(f"P99 latency: {df['latency_ms'].quantile(0.99):.3f} ms")

def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze benchmark client CSV logs")
    parser.add_argument(
        "directory",
        type=Path,
        nargs="?",
        default=Path.cwd(),
        help="Directory containing client-*.csv files (default: current directory)",
    )
    args = parser.parse_args()

    df = load_client_logs(args.directory)
    output_dir = args.directory / "plots"
    ensure_output_dir(output_dir)

    generated: list[Path] = []
    generated.extend(plot_latency_over_time(df, output_dir))
   
    print_summary(df)
    print(f"\nLoaded {len(df)} rows from {args.directory}")
    print("Generated files:")
    for path in generated:
        print(f" - {path}")

if __name__ == "__main__":
    main()
