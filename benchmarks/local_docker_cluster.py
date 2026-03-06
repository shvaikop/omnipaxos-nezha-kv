from __future__ import annotations

import subprocess
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path

import toml


@dataclass(frozen=True)
class FlexibleQuorum:
    read_quorum_size: int
    write_quorum_size: int


@dataclass(frozen=True)
class RequestInterval:
    duration_sec: int
    requests_per_sec: int
    read_ratio: float


@dataclass(frozen=True)
class LocalServerConfig:
    server_id: int
    location: str
    rust_log: str
    num_clients: int


@dataclass(frozen=True)
class LocalClientConfig:
    client_id: int
    server_id: int
    location: str
    rust_log: str
    requests: list[RequestInterval]


@dataclass(frozen=True)
class LocalClusterConfig:
    cluster_id: int
    nodes: list[int]
    initial_leader: int
    initial_flexible_quorum: FlexibleQuorum | None
    server_configs: dict[int, LocalServerConfig]
    client_configs: dict[int, LocalClientConfig]
    server_image: str
    client_image: str
    listen_port: int


class LocalDockerCluster:
    def __init__(self, cluster_config: LocalClusterConfig) -> None:
        self._cluster_config = cluster_config
        self._repo_root = Path(__file__).resolve().parents[1]
        self._network_name = f"opkv-bench-{cluster_config.cluster_id}"

    def run(self, logs_directory: Path, rebuild_images: bool = False) -> None:
        logs_directory = logs_directory.resolve()
        logs_directory.mkdir(parents=True, exist_ok=True)
        configs_dir = logs_directory / "_configs"
        configs_dir.mkdir(parents=True, exist_ok=True)
        print(f"[local-bench] Preparing run directory: {logs_directory}")

        if rebuild_images:
            print("[local-bench] Rebuilding Docker images")
            self._build_images()

        server_names = [self._server_container_name(sid) for sid in self._cluster_config.nodes]
        client_names = [self._client_container_name(cid) for cid in self._cluster_config.client_configs]
        all_names = server_names + client_names

        print("[local-bench] Cleaning up stale containers")
        self._cleanup_containers(all_names)
        self._ensure_network()

        cluster_cfg_path = configs_dir / "cluster-config.toml"
        cluster_cfg_path.write_text(self._cluster_toml(), encoding="utf-8")
        print(f"[local-bench] Wrote cluster config: {cluster_cfg_path}")

        try:
            server_ids = []
            for server_id in self._cluster_config.nodes:
                server_cfg = self._cluster_config.server_configs[server_id]
                server_cfg_path = configs_dir / f"server-{server_id}-config.toml"
                server_cfg_path.write_text(
                    self._server_toml(server_cfg, logs_directory), encoding="utf-8"
                )
                self._run_server(server_cfg, server_cfg_path, cluster_cfg_path, logs_directory)
                server_ids.append(server_id)

            print("[local-bench] Starting server processes")
            self._start_server_processes(server_ids)

            # Give servers time to establish peer links before clients start.
            time.sleep(2)
            print("[local-bench] Server warmup complete")

            client_ids = []
            for client_id in sorted(self._cluster_config.client_configs):
                client_cfg = self._cluster_config.client_configs[client_id]
                client_cfg_path = configs_dir / f"client-{client_id}-config.toml"
                client_cfg_path.write_text(
                    self._client_toml(client_cfg, logs_directory), encoding="utf-8"
                )
                self._run_client(client_cfg, client_cfg_path, logs_directory)
                client_ids.append(client_id)

            print("[local-bench] Starting client processes")
            client_processes = self._start_client_processes(client_ids)
            print("[local-bench] Waiting for clients to finish")
            self._wait_for_clients(client_processes)
            print("[local-bench] Clients finished successfully")
        finally:
            print("[local-bench] Cleaning up containers and network")
            self._cleanup_containers(all_names)
            self._remove_network()

    def change_cluster_config(self, **kwargs) -> None:
        new_config = replace(self._cluster_config, **kwargs)
        self._cluster_config = new_config

    def shutdown(self) -> None:
        names = [self._server_container_name(sid) for sid in self._cluster_config.nodes]
        names.extend(
            [self._client_container_name(cid) for cid in self._cluster_config.client_configs]
        )
        self._cleanup_containers(names)
        self._remove_network()

    def _build_images(self) -> None:
        workspace_root = self._repo_root.parent
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                self._cluster_config.server_image,
                "-f",
                str(self._repo_root / "server.dockerfile"),
                str(workspace_root),
            ],
            check=True,
        )
        print(f"[local-bench] Built image: {self._cluster_config.server_image}")
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                self._cluster_config.client_image,
                "-f",
                str(self._repo_root / "client.dockerfile"),
                str(workspace_root),
            ],
            check=True,
        )
        print(f"[local-bench] Built image: {self._cluster_config.client_image}")

    def _cluster_toml(self) -> str:
        cluster_dict = {
            "nodes": self._cluster_config.nodes,
            "node_addrs": [
                f"{self._server_container_name(server_id)}:{self._cluster_config.listen_port}"
                for server_id in self._cluster_config.nodes
            ],
            "initial_leader": self._cluster_config.initial_leader,
        }
        if self._cluster_config.initial_flexible_quorum is not None:
            cluster_dict["initial_flexible_quorum"] = asdict(
                self._cluster_config.initial_flexible_quorum
            )
        return toml.dumps(cluster_dict)

    def _server_toml(self, config: LocalServerConfig, logs_dir: Path) -> str:
        cfg = {
            "location": config.location,
            "server_id": config.server_id,
            "listen_address": "0.0.0.0",
            "listen_port": self._cluster_config.listen_port,
            "num_clients": config.num_clients,
            "output_filepath": str(logs_dir / f"server-{config.server_id}.json"),
        }
        return toml.dumps(cfg)

    def _client_toml(self, config: LocalClientConfig, logs_dir: Path) -> str:
        cfg = {
            "location": config.location,
            "server_id": config.server_id,
            "server_address": (
                f"{self._server_container_name(config.server_id)}:{self._cluster_config.listen_port}"
            ),
            "summary_filepath": str(logs_dir / f"client-{config.client_id}.json"),
            "output_filepath": str(logs_dir / f"client-{config.client_id}.csv"),
            "requests": [asdict(r) for r in config.requests],
        }
        return toml.dumps(cfg)

    def _run_server(
        self,
        config: LocalServerConfig,
        server_cfg_path: Path,
        cluster_cfg_path: Path,
        logs_dir: Path,
    ) -> None:
        subprocess.run(
            [
                "docker",
                "run",
                "--init",
                "-d",
                "--name",
                self._server_container_name(config.server_id),
                "--network",
                self._network_name,
                "--env",
                f"RUST_LOG={config.rust_log}",
                "--env",
                "SERVER_CONFIG_FILE=/server-config.toml",
                "--env",
                "CLUSTER_CONFIG_FILE=/cluster-config.toml",
                "--volume",
                f"{server_cfg_path}:/server-config.toml:ro",
                "--volume",
                f"{cluster_cfg_path}:/cluster-config.toml:ro",
                "--volume",
                f"{logs_dir}:{logs_dir}",
                self._cluster_config.server_image,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        print(f"[local-bench] Started server container: {self._server_container_name(config.server_id)}")
        self._assert_container_running(self._server_container_name(config.server_id))

    def _start_server_processes(self, server_ids: list[int]) -> None:
        for server_id in server_ids:
            self._start_server_process(server_id)

    def _start_server_process(self, server_id: int) -> None:
        container_name = self._server_container_name(server_id)
        subprocess.run(
            [
                "docker",
                "exec",
                "-d",
                container_name,
                "sh",
                "-lc",
                "/usr/local/bin/server > /tmp/server.log 2>&1",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        print(f"[local-bench] Launched server process in: {container_name}")
        time.sleep(1)
        logs_result = subprocess.run(
            ["docker", "exec", container_name, "cat", "/tmp/server.log"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        logs = logs_result.stdout.strip()

    def _run_client(
        self, config: LocalClientConfig, client_cfg_path: Path, logs_dir: Path
    ) -> None:
        subprocess.run(
            [
                "docker",
                "run",
                "--init",
                "-d",
                "--name",
                self._client_container_name(config.client_id),
                "--network",
                self._network_name,
                "--env",
                f"RUST_LOG={config.rust_log}",
                "--env",
                "CONFIG_FILE=/client-config.toml",
                "--volume",
                f"{client_cfg_path}:/client-config.toml:ro",
                "--volume",
                f"{logs_dir}:{logs_dir}",
                self._cluster_config.client_image,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        print(f"[local-bench] Started client container: {self._client_container_name(config.client_id)}")
        self._assert_container_running(self._client_container_name(config.client_id))

    def _start_client_processes(
        self, client_ids: list[int]
    ) -> list[tuple[int, subprocess.Popen[str]]]:
        client_processes: list[tuple[int, subprocess.Popen[str]]] = []
        for client_id in client_ids:
            container_name = self._client_container_name(client_id)
            process = subprocess.Popen(
                [
                    "docker",
                    "exec",
                    container_name,
                    "sh",
                    "-lc",
                    "/usr/local/bin/client > /tmp/client.log 2>&1",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            client_processes.append((client_id, process))
            print(f"[local-bench] Launched client process in: {container_name}")
        return client_processes

    def _wait_for_clients(
        self, client_processes: list[tuple[int, subprocess.Popen[str]]]
    ) -> None:
        for client_id, process in client_processes:
            _stdout, stderr = process.communicate()
            if process.returncode == 0:
                print(f"[local-bench] Client finished: {self._client_container_name(client_id)}")
                continue
            container_name = self._client_container_name(client_id)
            logs_result = subprocess.run(
                ["docker", "exec", container_name, "cat", "/tmp/client.log"],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            logs = logs_result.stdout.strip()
            raise RuntimeError(
                f"Client process failed in {container_name} with returncode={process.returncode}. "
                f"docker exec stderr:\n{stderr or ''}\nClient logs:\n{logs}"
            )

    def _ensure_network(self) -> None:
        inspect = subprocess.run(
            ["docker", "network", "inspect", self._network_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if inspect.returncode != 0:
            subprocess.run(
                ["docker", "network", "create", self._network_name],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            print(f"[local-bench] Created network: {self._network_name}")
        else:
            print(f"[local-bench] Reusing network: {self._network_name}")

    def _remove_network(self) -> None:
        subprocess.run(
            ["docker", "network", "rm", self._network_name],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print(f"[local-bench] Removed network: {self._network_name}")

    @staticmethod
    def _cleanup_containers(container_names: list[str]) -> None:
        for container_name in container_names:
            subprocess.run(
                ["docker", "rm", "-f", container_name],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    @staticmethod
    def _assert_container_running(container_name: str) -> None:
        # Give the process a moment to fail fast if startup is invalid.
        time.sleep(1)
        status_result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", container_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        if status_result.returncode != 0:
            raise RuntimeError(f"Container {container_name} disappeared after startup")
        status = status_result.stdout.strip()
        if status == "running":
            return

        exit_code_result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.ExitCode}}", container_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        exit_code = (
            exit_code_result.stdout.strip() if exit_code_result.returncode == 0 else "unknown"
        )
        logs_result = subprocess.run(
            ["docker", "logs", container_name],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        logs = logs_result.stdout.strip()
        raise RuntimeError(
            f"Container {container_name} exited immediately with status={status}, "
            f"exit_code={exit_code}. Logs:\n{logs}"
        )

    def _server_container_name(self, server_id: int) -> str:
        return f"opkv-c{self._cluster_config.cluster_id}-server-{server_id}"

    def _client_container_name(self, client_id: int) -> str:
        return f"opkv-c{self._cluster_config.cluster_id}-client-{client_id}"


class LocalDockerClusterBuilder:
    def __init__(
        self,
        cluster_id: int,
        server_image: str = "omnipaxos-server",
        client_image: str = "omnipaxos-client",
        listen_port: int = 8000,
    ) -> None:
        self._cluster_id = cluster_id
        self._server_image = server_image
        self._client_image = client_image
        self._listen_port = listen_port
        self._server_configs: dict[int, LocalServerConfig] = {}
        self._client_configs: dict[int, LocalClientConfig] = {}
        self._initial_leader: int | None = None
        self._initial_quorum: FlexibleQuorum | None = None

    def server(self, server_id: int, rust_log: str = "info") -> LocalDockerClusterBuilder:
        if server_id in self._server_configs:
            raise ValueError(f"Server {server_id} already exists")
        self._server_configs[server_id] = LocalServerConfig(
            server_id=server_id,
            location=f"local-{server_id}",
            rust_log=rust_log,
            num_clients=0,
        )
        return self

    def client(
        self,
        client_id: int,
        server_id: int,
        requests: list[RequestInterval] | None = None,
        rust_log: str = "info",
    ) -> LocalDockerClusterBuilder:
        if client_id in self._client_configs:
            raise ValueError(f"Client {client_id} already exists")
        self._client_configs[client_id] = LocalClientConfig(
            client_id=client_id,
            server_id=server_id,
            location=f"local-{client_id}",
            rust_log=rust_log,
            requests=requests if requests is not None else [],
        )
        return self

    def initial_leader(self, initial_leader: int) -> LocalDockerClusterBuilder:
        self._initial_leader = initial_leader
        return self

    def initial_quorum(self, flex_quorum: FlexibleQuorum) -> LocalDockerClusterBuilder:
        self._initial_quorum = flex_quorum
        return self

    def build(self) -> LocalDockerCluster:
        if self._initial_leader is None:
            raise ValueError("Need to set cluster's initial leader")
        if not self._server_configs:
            raise ValueError("Need at least one server")

        for client in self._client_configs.values():
            if client.server_id not in self._server_configs:
                raise ValueError(
                    f"Client {client.client_id} references missing server {client.server_id}"
                )

        client_count_by_server: dict[int, int] = {sid: 0 for sid in self._server_configs}
        for client_cfg in self._client_configs.values():
            client_count_by_server[client_cfg.server_id] += 1

        server_configs = {
            sid: replace(cfg, num_clients=client_count_by_server[sid])
            for sid, cfg in self._server_configs.items()
        }

        nodes = sorted(server_configs.keys())
        if self._initial_leader not in nodes:
            raise ValueError(
                f"Initial leader {self._initial_leader} must be one of server ids {nodes}"
            )

        cluster_config = LocalClusterConfig(
            cluster_id=self._cluster_id,
            nodes=nodes,
            initial_leader=self._initial_leader,
            initial_flexible_quorum=self._initial_quorum,
            server_configs=server_configs,
            client_configs=self._client_configs,
            server_image=self._server_image,
            client_image=self._client_image,
            listen_port=self._listen_port,
        )
        print(cluster_config)
        return LocalDockerCluster(cluster_config)
