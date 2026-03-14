import argparse
from pathlib import Path

from local_docker_cluster import (
    FlexibleQuorum,
    LocalDockerClusterBuilder,
    RequestInterval,
)


def example_workload() -> dict[int, list[RequestInterval]]:
    experiment_duration = 10
    read_ratio = 0.50
    high_load = RequestInterval(experiment_duration, 100, read_ratio)
    low_load = RequestInterval(experiment_duration, 10, read_ratio)

    nodes = [1, 2, 3]
    workload = {}
    for node in nodes:
        if node == 1:
            requests = [high_load, low_load]
        else:
            requests = [low_load, high_load]
        workload[node] = requests
    return workload

def five_example_benchmark(num_runs: int = 3, rebuild_images: bool = False) -> None:
    workload = example_workload()
    cluster = (
        LocalDockerClusterBuilder(1)
        .initial_leader(1)
        .server(1)
        .server(2)
        .server(3)
        .server(4)
        .server(5)
        .client(1, server_id=1, requests=workload[1])
        .client(2, server_id=2, requests=workload[2])
        .client(3, server_id=3, requests=workload[3])
    ).build()
    experiment_log_dir = Path("./logs/local-example-experiment")

    majority_quorum = FlexibleQuorum(read_quorum_size=3, write_quorum_size=3)
    flex_quorum = FlexibleQuorum(read_quorum_size=4, write_quorum_size=1)
    for run in range(num_runs):
        cluster.change_cluster_config(initial_flexible_quorum=majority_quorum)
        iteration_dir = Path.joinpath(experiment_log_dir, f"MajorityQuorum/run-{run}")
        print("RUNNING:", iteration_dir)
        cluster.run(iteration_dir, rebuild_images=rebuild_images and run == 0)

        cluster.change_cluster_config(initial_flexible_quorum=flex_quorum)
        iteration_dir = Path.joinpath(experiment_log_dir, f"FlexQuorum/run-{run}")
        print("RUNNING:", iteration_dir)
        cluster.run(iteration_dir, rebuild_images=False)

    import pdb; pdb.set_trace()
    cluster.shutdown()

def example_benchmark(num_runs: int = 3, rebuild_images: bool = False) -> None:
    workload = example_workload()
    cluster = (
        LocalDockerClusterBuilder(1)
        .initial_leader(1)
        .server(1)
        .server(2)
        .server(3)
        .client(1, server_id=1, requests=workload[1])
        .client(2, server_id=2, requests=workload[2])
        .client(3, server_id=3, requests=workload[3])
    ).build()
    experiment_log_dir = Path("./logs/local-example-experiment")

    majority_quorum = FlexibleQuorum(read_quorum_size=2, write_quorum_size=2)
    flex_quorum = FlexibleQuorum(read_quorum_size=3, write_quorum_size=1)
    for run in range(num_runs):
        cluster.change_cluster_config(initial_flexible_quorum=majority_quorum)
        iteration_dir = Path.joinpath(experiment_log_dir, f"MajorityQuorum/run-{run}")
        print("RUNNING:", iteration_dir)
        cluster.run(iteration_dir, rebuild_images=rebuild_images and run == 0)

        cluster.change_cluster_config(initial_flexible_quorum=flex_quorum)
        iteration_dir = Path.joinpath(experiment_log_dir, f"FlexQuorum/run-{run}")
        print("RUNNING:", iteration_dir)
        cluster.run(iteration_dir, rebuild_images=False)

    import pdb; pdb.set_trace()
    cluster.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local Docker benchmarks")
    parser.add_argument("--runs", type=int, default=3, help="Number of benchmark runs")
    parser.add_argument(
        "--rebuild-images",
        action="store_true",
        help="Rebuild server/client Docker images before first run",
    )
    args = parser.parse_args()
    # example_benchmark(num_runs=args.runs, rebuild_images=args.rebuild_images)
    five_example_benchmark(num_runs=args.runs, rebuild_images=args.rebuild_images)

if __name__ == "__main__":
    main()
