#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import venv
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO


BLUE = "\033[0;34m"
GREEN = "\033[0;32m"
RED = "\033[0;31m"
RESET = "\033[0m"
YELLOW = "\033[1;33m"
SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
REPO_ROOT = SCRIPT_DIR.parent.parent
VENV_DIR = SCRIPT_DIR / ".venv" / "app-start-testing"
VENV_PYTHON = VENV_DIR / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
VENV_BOOTSTRAP_ENV = "APP_START_TESTING_VENV_BOOTSTRAPPED"
VENV_DISABLE_ENV = "APP_START_TESTING_DISABLE_VENV"


class AppStartTestingError(Exception):
    pass


@dataclass(frozen=True)
class Config:
    logger_cli: str
    logger_host: str
    logger_port: int
    base_port: int
    api_key: str
    api_url: str
    app_id: str
    app_version: str
    app_version_code: str
    platform: str
    model: str
    app_start_os: str
    observe_stats_action_id: str
    observe_stats_output_base: str
    output_directory_base: Path
    expected_uploaded_counter_value: int | None
    max_iterations: int
    parallelism: int
    clear_data_dir_percent: int
    sdk_directory_base: Path
    startup_timeout_seconds: int
    shutdown_timeout_seconds: int
    startup_sleep_seconds: float


@dataclass(frozen=True)
class WorkerConfig:
    worker_index: int
    worker_iterations: int
    port: int
    sdk_directory: Path
    log_output_path: Path
    observe_output_path: Path | None
    config: Config


@dataclass(frozen=True)
class WorkerResult:
    worker_index: int
    log_output_path: Path


def colorize(color: str, label: str) -> str:
    if sys.stdout.isatty():
      return f"{color}{label}{RESET}"
    return label


def print_header(message: str) -> None:
    border = "=" * 65
    print("")
    print(colorize(BLUE, border))
    print(colorize(BLUE, message))
    print(colorize(BLUE, border))


def print_info(message: str) -> None:
    print(f"{colorize(GREEN, '[INFO]')} {message}")


def print_warning(message: str) -> None:
    print(f"{colorize(YELLOW, '[WARN]')} {message}")


def print_error(message: str) -> None:
    print(f"{colorize(RED, '[ERROR]')} {message}", file=sys.stderr)


def ensure_virtualenv() -> None:
    if os.environ.get(VENV_DISABLE_ENV) == "1":
        return
    if os.environ.get(VENV_BOOTSTRAP_ENV) == "1":
        return
    if sys.prefix != getattr(sys, "base_prefix", sys.prefix):
        return

    try:
        if not VENV_PYTHON.exists():
            VENV_DIR.parent.mkdir(parents=True, exist_ok=True)
            builder = venv.EnvBuilder(with_pip=False, clear=False, symlinks=os.name != "nt")
            builder.create(VENV_DIR)
        os.environ[VENV_BOOTSTRAP_ENV] = "1"
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), str(SCRIPT_PATH), *sys.argv[1:]])
    except Exception as exc:
        print_warning(f"venv bootstrap skipped: {exc}")


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def percent_int(value: str) -> int:
    parsed = non_negative_int(value)
    if parsed > 100:
        raise argparse.ArgumentTypeError("must be an integer between 0 and 100")
    return parsed


def port_int(value: str) -> int:
    parsed = positive_int(value)
    if parsed > 65535:
        raise argparse.ArgumentTypeError("must be an integer between 1 and 65535")
    return parsed


def non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative number") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative number")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    env = os.environ
    logger_port = env.get("LOGGER_PORT", "5501")
    base_port = env.get("BASE_PORT", logger_port)
    parser = argparse.ArgumentParser(
        description="Repeatedly start logger-cli, emit a lifecycle AppStart log, then flush and shut down.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Environment overrides: LOGGER_CLI, LOGGER_HOST, LOGGER_PORT, API_KEY, API_URL, APP_ID,\n"
            "APP_VERSION, APP_VERSION_CODE, PLATFORM, MODEL, APP_START_OS,\n"
            "OBSERVE_STATS_ACTION_ID, OBSERVE_STATS_OUTPUT_BASE, OUTPUT_DIRECTORY_BASE,\n"
            "EXPECTED_UPLOADED_COUNTER_VALUE, BASE_PORT, PARALLELISM, SDK_DIRECTORY_BASE"
        ),
    )
    parser.add_argument("--max-iterations", type=positive_int, default=positive_int(env.get("MAX_ITERATIONS", "10")))
    parser.add_argument("--parallelism", type=positive_int, default=positive_int(env.get("PARALLELISM", "1")))
    parser.add_argument("--base-port", type=port_int, default=port_int(base_port))
    parser.add_argument("--sdk-directory-base", default=env.get("SDK_DIRECTORY_BASE", ""))
    parser.add_argument("--observe-stats-action-id", default=env.get("OBSERVE_STATS_ACTION_ID", ""))
    parser.add_argument("--observe-stats-output-base", default=env.get("OBSERVE_STATS_OUTPUT_BASE", ""))
    parser.add_argument("--output-directory-base", default=env.get("OUTPUT_DIRECTORY_BASE", ""))
    parser.add_argument(
        "--expect-uploaded-counter-value",
        type=non_negative_int,
        default=(
            non_negative_int(env["EXPECTED_UPLOADED_COUNTER_VALUE"])
            if env.get("EXPECTED_UPLOADED_COUNTER_VALUE")
            else None
        ),
    )
    parser.add_argument(
        "--clear-data-dir-percent",
        type=percent_int,
        default=percent_int(env.get("CLEAR_DATA_DIR_PERCENT", "0")),
    )
    parser.add_argument(
        "--startup-timeout",
        dest="startup_timeout_seconds",
        type=positive_int,
        default=positive_int(env.get("STARTUP_TIMEOUT_SECONDS", "15")),
    )
    parser.add_argument(
        "--shutdown-timeout",
        dest="shutdown_timeout_seconds",
        type=positive_int,
        default=positive_int(env.get("SHUTDOWN_TIMEOUT_SECONDS", "15")),
    )
    parser.add_argument(
        "--startup-sleep",
        dest="startup_sleep_seconds",
        type=non_negative_float,
        default=non_negative_float(env.get("STARTUP_SLEEP_SECONDS", "1")),
    )
    return parser


def default_sdk_directory_base() -> Path:
    return REPO_ROOT / ".tmp" / "logger-cli-app-start-workers"


def default_output_directory_base(sdk_directory_base: Path, observe_stats_output_base: str) -> Path:
    if observe_stats_output_base:
        return Path(observe_stats_output_base)
    return sdk_directory_base / "output"


def build_config(args: argparse.Namespace) -> Config:
    env = os.environ
    sdk_directory_base = Path(args.sdk_directory_base) if args.sdk_directory_base else default_sdk_directory_base()
    output_directory_base = (
        Path(args.output_directory_base)
        if args.output_directory_base
        else default_output_directory_base(sdk_directory_base, args.observe_stats_output_base)
    )
    config = Config(
        logger_cli=env.get("LOGGER_CLI", "logger-cli"),
        logger_host=env.get("LOGGER_HOST", "localhost"),
        logger_port=port_int(env.get("LOGGER_PORT", "5501")),
        base_port=args.base_port,
        api_key=env.get("API_KEY", "your-api-key-here"),
        api_url=env.get("API_URL", "https://api.bitdrift.io"),
        app_id=env.get("APP_ID", "io.bitdrift.cli"),
        app_version=env.get("APP_VERSION", "1.0.0"),
        app_version_code=env.get("APP_VERSION_CODE", "10"),
        platform=env.get("PLATFORM", "android"),
        model=env.get("MODEL", "Pixel-8"),
        app_start_os=env.get("APP_START_OS", "Android"),
        observe_stats_action_id=args.observe_stats_action_id,
        observe_stats_output_base=args.observe_stats_output_base,
        output_directory_base=output_directory_base,
        expected_uploaded_counter_value=args.expect_uploaded_counter_value,
        max_iterations=args.max_iterations,
        parallelism=args.parallelism,
        clear_data_dir_percent=args.clear_data_dir_percent,
        sdk_directory_base=sdk_directory_base,
        startup_timeout_seconds=args.startup_timeout_seconds,
        shutdown_timeout_seconds=args.shutdown_timeout_seconds,
        startup_sleep_seconds=args.startup_sleep_seconds,
    )
    validate_config(config)
    return config


def validate_config(config: Config) -> None:
    if config.api_key == "your-api-key-here":
        raise AppStartTestingError("set API_KEY before running this script")
    if config.base_port + config.parallelism - 1 > 65535:
        raise AppStartTestingError("--base-port plus --parallelism exceeds port 65535")
    if config.expected_uploaded_counter_value is not None and not config.observe_stats_action_id:
        raise AppStartTestingError("--expect-uploaded-counter-value requires --observe-stats-action-id")
    if shutil.which(config.logger_cli) is None:
        raise AppStartTestingError(f"required command not found: {config.logger_cli}")


class WorkerLogger:
    def __init__(self, worker_index: int, log_path: Path) -> None:
        self.worker_index = worker_index
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _write(self, prefix: str, message: str) -> None:
        line = f"{prefix} {message}"
        with self._lock:
            with self.log_path.open("a", encoding="utf-8") as handle:
                handle.write(line)
                handle.write("\n")

    def header(self, message: str) -> None:
        border = "=" * 65
        self._write("", "")
        self._write("", border)
        self._write("", message)
        self._write("", border)

    def info(self, message: str) -> None:
        self._write("[INFO]", message)

    def warning(self, message: str) -> None:
        self._write("[WARN]", message)

    def error(self, message: str) -> None:
        self._write("[ERROR]", message)


def wait_for_port(host: str, port: int, timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(1)
    return False


def wait_for_process_exit(process: subprocess.Popen[str], timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return True
        time.sleep(1)
    return process.poll() is not None


def should_clean_data_dir(percent: int) -> bool:
    return random.randrange(100) < percent


def worker_observe_output_path(config: Config, worker_index: int, sdk_directory: Path) -> Path | None:
    if not config.observe_stats_action_id:
        return None
    if config.observe_stats_output_base:
        return Path(config.observe_stats_output_base) / f"worker-{worker_index}.jsonl"
    return sdk_directory / "stats-observation" / "metrics.jsonl"


def worker_log_output_path(config: Config, worker_index: int) -> Path:
    return config.output_directory_base / f"worker-{worker_index}.log"


def prepare_observe_output_path(observe_output_path: Path) -> None:
    observe_output_path.parent.mkdir(parents=True, exist_ok=True)
    if observe_output_path.exists():
        observe_output_path.unlink()


def prepare_worker_log_output_path(log_output_path: Path) -> None:
    log_output_path.parent.mkdir(parents=True, exist_ok=True)
    log_output_path.write_text("", encoding="utf-8")


def assert_uploaded_counter_value(
    observe_output_path: Path,
    worker_index: int,
    iteration: int,
    expected_uploaded_counter_value: int,
    observe_stats_action_id: str,
    logger: WorkerLogger,
) -> None:
    if not observe_output_path.is_file():
        raise AppStartTestingError(
            f"worker {worker_index} iteration {iteration} did not create {observe_output_path}"
        )

    records: list[dict] = []
    with observe_output_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise AppStartTestingError(
                    f"worker {worker_index} iteration {iteration} wrote invalid JSON to {observe_output_path}: {exc}"
                ) from exc

    upload_uuid = ""
    for record in records:
        if record.get("phase") != "upload_attempt":
            continue
        metrics = record.get("metrics") or []
        for metric in metrics:
            if metric.get("metric_type") == "counter" and metric.get("value") == expected_uploaded_counter_value:
                upload_uuid = record.get("upload_uuid") or ""
                break

    if not upload_uuid:
        for record in records:
            logger.error(json.dumps(record, separators=(",", ":")))
        raise AppStartTestingError(
            "worker "
            f"{worker_index} iteration {iteration} did not upload action ID {observe_stats_action_id} "
            f"with counter value {expected_uploaded_counter_value}"
        )

    for record in records:
        if (
            record.get("phase") == "upload_ack"
            and record.get("upload_uuid") == upload_uuid
            and record.get("success") is True
        ):
            logger.info(
                "worker "
                f"{worker_index} iteration {iteration} observed acknowledged upload for action ID "
                f"{observe_stats_action_id} with counter value {expected_uploaded_counter_value}"
            )
            return

    for record in records:
        logger.error(json.dumps(record, separators=(",", ":")))
    raise AppStartTestingError(
        "worker "
        f"{worker_index} iteration {iteration} uploaded action ID {observe_stats_action_id} "
        f"with counter value {expected_uploaded_counter_value}, but no successful ack was observed"
    )


def build_start_command(config: Config, port: int, sdk_directory: Path, clean_flag: bool, observe_output_path: Path | None) -> list[str]:
    command = [
        config.logger_cli,
        "--host",
        config.logger_host,
        "--port",
        str(port),
        "--sdk-directory",
        str(sdk_directory),
        "start",
        "--api-key",
        config.api_key,
        "--api-url",
        config.api_url,
        "--app-id",
        config.app_id,
        "--app-version",
        config.app_version,
        "--app-version-code",
        config.app_version_code,
        "--platform",
        config.platform,
        "--model",
        config.model,
    ]
    if config.observe_stats_action_id:
        command.extend(["--observe-stats-action-id", config.observe_stats_action_id])
    if observe_output_path is not None:
        command.extend(["--observe-stats-output", str(observe_output_path)])
    if clean_flag:
        command.append("--clean-data-dir")
    return command


def send_app_start(config: Config, port: int) -> None:
    command = [
        config.logger_cli,
        "--host",
        config.logger_host,
        "--port",
        str(port),
        "log",
        "--log-type",
        "lifecycle",
        "--log-level",
        "info",
        "--field",
        "os",
        config.app_start_os,
        "AppStart",
    ]
    subprocess.run(command, check=True)


def shutdown_logger(process: subprocess.Popen[str], timeout_seconds: int) -> None:
    process.send_signal(signal.SIGINT)
    if not wait_for_process_exit(process, timeout_seconds):
        process.terminate()
        raise AppStartTestingError(
            f"logger process {process.pid} did not exit within {timeout_seconds}s"
        )
    return_code = process.wait()
    if return_code != 0:
        raise AppStartTestingError(f"logger process {process.pid} exited with status {return_code}")


def iterations_for_worker(worker_index: int, total_workers: int, total_iterations: int) -> int:
    base_iterations = total_iterations // total_workers
    remainder = total_iterations % total_workers
    return base_iterations + 1 if worker_index <= remainder else base_iterations


def build_worker_configs(config: Config) -> list[WorkerConfig]:
    workers: list[WorkerConfig] = []
    for worker_index in range(1, config.parallelism + 1):
        worker_iterations = iterations_for_worker(worker_index, config.parallelism, config.max_iterations)
        if worker_iterations <= 0:
            continue
        port = config.base_port + worker_index - 1
        sdk_directory = config.sdk_directory_base / f"worker-{worker_index}"
        log_output_path = worker_log_output_path(config, worker_index)
        workers.append(
            WorkerConfig(
                worker_index=worker_index,
                worker_iterations=worker_iterations,
                port=port,
                sdk_directory=sdk_directory,
                log_output_path=log_output_path,
                observe_output_path=worker_observe_output_path(config, worker_index, sdk_directory),
                config=config,
            )
        )
    return workers


def run_worker(worker: WorkerConfig) -> WorkerResult:
    logger = WorkerLogger(worker.worker_index, worker.log_output_path)
    logger.header(f"Worker {worker.worker_index}")
    logger.info(f"worker port: {worker.config.logger_host}:{worker.port}")
    logger.info(f"worker sdk directory: {worker.sdk_directory}")
    logger.info(f"worker iterations: {worker.worker_iterations}")
    logger.info(f"worker log output: {worker.log_output_path}")
    if worker.config.observe_stats_action_id:
        logger.info(f"worker observed action ID: {worker.config.observe_stats_action_id}")
        if worker.observe_output_path is not None:
            logger.info(f"worker stats output: {worker.observe_output_path}")

    worker_clean_count = 0
    for iteration in range(1, worker.worker_iterations + 1):
        clean_flag = should_clean_data_dir(worker.config.clear_data_dir_percent)
        if clean_flag:
            worker_clean_count += 1

        logger.info(
            f"worker {worker.worker_index} iteration {iteration}/{worker.worker_iterations} clean={str(clean_flag).lower()}"
        )

        if worker.config.expected_uploaded_counter_value is not None and worker.observe_output_path is not None:
            prepare_observe_output_path(worker.observe_output_path)

        worker.sdk_directory.mkdir(parents=True, exist_ok=True)

        with worker.log_output_path.open("a", encoding="utf-8") as log_handle:
            process = subprocess.Popen(
                build_start_command(
                    worker.config,
                    worker.port,
                    worker.sdk_directory,
                    clean_flag,
                    worker.observe_output_path,
                ),
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                text=True,
            )
            try:
                if not wait_for_port(
                    worker.config.logger_host,
                    worker.port,
                    worker.config.startup_timeout_seconds,
                ):
                    process.terminate()
                    process.wait(timeout=5)
                    raise AppStartTestingError(
                        "worker "
                        f"{worker.worker_index} logger did not open {worker.config.logger_host}:{worker.port} "
                        f"within {worker.config.startup_timeout_seconds}s"
                    )

                time.sleep(worker.config.startup_sleep_seconds)
                send_app_start(worker.config, worker.port)
                shutdown_logger(process, worker.config.shutdown_timeout_seconds)
            except subprocess.CalledProcessError as exc:
                raise AppStartTestingError(
                    f"worker {worker.worker_index} failed while invoking logger-cli: {exc}"
                ) from exc
            except subprocess.TimeoutExpired as exc:
                process.kill()
                raise AppStartTestingError(
                    f"worker {worker.worker_index} logger process did not stop cleanly: {exc}"
                ) from exc
            except Exception:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                raise

        if worker.config.expected_uploaded_counter_value is not None and worker.observe_output_path is not None:
            assert_uploaded_counter_value(
                worker.observe_output_path,
                worker.worker_index,
                iteration,
                worker.config.expected_uploaded_counter_value,
                worker.config.observe_stats_action_id,
                logger,
            )

        logger.info(f"worker {worker.worker_index} iteration {iteration} complete")

    logger.info(f"worker {worker.worker_index} complete; data directory resets: {worker_clean_count}")
    return WorkerResult(worker_index=worker.worker_index, log_output_path=worker.log_output_path)


def run_workers(config: Config) -> None:
    workers = build_worker_configs(config)
    if not workers:
        return

    failures: list[tuple[int, Path, Exception]] = []
    with ThreadPoolExecutor(max_workers=len(workers)) as executor:
        future_to_worker = {executor.submit(run_worker, worker): worker for worker in workers}
        for future in as_completed(future_to_worker):
            worker = future_to_worker[future]
            try:
                result = future.result()
            except Exception as exc:
                failures.append((worker.worker_index, worker.log_output_path, exc))
                print_error(f"worker {worker.worker_index} failed; log: {worker.log_output_path}")
            else:
                print_info(f"worker {result.worker_index} succeeded; log: {result.log_output_path}")

    if failures:
        first_worker_index, _, first_error = failures[0]
        raise AppStartTestingError(f"worker {first_worker_index} failed: {first_error}")


def print_run_summary(config: Config) -> None:
    print_header("AppStart Iteration Test")
    print_info(f"iterations: {config.max_iterations}")
    print_info(f"parallelism: {config.parallelism}")
    print_info(f"base port: {config.base_port}")
    print_info(f"clear data dir chance: {config.clear_data_dir_percent}%")
    print_info(f"AppStart os field: {config.app_start_os}")
    if config.observe_stats_action_id:
        print_info(f"observed action ID: {config.observe_stats_action_id}")
        if config.observe_stats_output_base:
            print_info(f"observer output base: {config.observe_stats_output_base}")
    if config.expected_uploaded_counter_value is not None:
        print_info(f"expected uploaded counter value: {config.expected_uploaded_counter_value}")
    if config.parallelism == 1:
        print_info(f"target logger: {config.logger_host}:{config.base_port}")
    else:
        print_info(
            f"worker port range: {config.logger_host}:{config.base_port}-{config.base_port + config.parallelism - 1}"
        )
    print_info(f"SDK directory base: {config.sdk_directory_base}")
    print_info(f"output directory base: {config.output_directory_base}")


def main(argv: list[str] | None = None) -> int:
    ensure_virtualenv()
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = build_config(args)
        for worker in build_worker_configs(config):
            prepare_worker_log_output_path(worker.log_output_path)
            print_info(
                f"starting worker {worker.worker_index} on {config.logger_host}:{worker.port}; "
                f"log: {worker.log_output_path}"
            )
        print_run_summary(config)
        run_workers(config)
    except AppStartTestingError as exc:
        print_error(str(exc))
        return 1

    print_header("Run Complete")
    print_info(f"completed iterations: {config.max_iterations}")
    print_info(f"parallel workers: {config.parallelism}")
    return 0


if __name__ == "__main__":
    sys.exit(main())