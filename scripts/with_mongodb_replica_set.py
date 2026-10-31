"""Run an existing command against an owned, bounded Docker replica-set fixture.

The image is preloaded (the CI standalone service uses the same pinned image).
This module owns lifecycle only; differential selection/oracles stay in their runners.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
import uuid

from pathlib import Path
from types import SimpleNamespace


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_mongodb_real_differential import inspect_server_runtime  # noqa: E402


_OWNER_LABEL = "mongoeco.differential-fixture"
_CANCELLATION = SimpleNamespace(depth=0, pending=None)


@contextlib.contextmanager
def _defer_cancellation():
    # Python can dispatch a handler queued by another thread even while the
    # main thread masks the signal. Defer the handler as well as OS delivery.
    _CANCELLATION.depth += 1
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        _CANCELLATION.depth -= 1
        if _CANCELLATION.depth == 0 and _CANCELLATION.pending is not None:
            signum, _CANCELLATION.pending = _CANCELLATION.pending, None
            raise _FixtureCancelledError(signum)


def _docker(*args, check=True, timeout=30):
    executable = shutil.which("docker")
    if executable is None:
        message = "Docker CLI is not available"
        raise RuntimeError(message)
    with _defer_cancellation():
        return subprocess.run(  # noqa: S603 - Docker argv, never shell text
            [executable, *args],
            check=check,
            capture_output=True,
            text=True,
            timeout=timeout,
        )


class ReplicaSetFixture:
    def __init__(self, image, target, artifacts, *, startup_timeout=90):
        if not re.fullmatch(r"[^\s-][^\s]*@sha256:[0-9a-f]{64}", image):
            message = "replica-set fixture requires a digest-pinned image"
            raise ValueError(message)
        self.image = image
        self.target = target
        self.artifacts = Path(artifacts)
        self.startup_timeout = startup_timeout
        self.owner = uuid.uuid4().hex
        self.name = "mongoeco-differential-" + self.owner
        self.container_id = None
        self.uri = None
        self._startup_deadline = None
        self.report = {
            "image": image,
            "target": target,
            "name": self.name,
            "owner": self.owner,
            "status": "starting",
            "cleanup": {},
        }

    def record(self):
        self.artifacts.mkdir(parents=True, exist_ok=True)
        (self.artifacts / "fixture.json").write_text(
            json.dumps(self.report, indent=2, default=str) + "\n"
        )

    def _command(self, *args, check=True):
        timeout = 30
        if self._startup_deadline is not None:
            timeout = min(timeout, self._startup_deadline - time.monotonic())
            if timeout <= 0:
                message = "replica-set startup deadline exceeded"
                raise TimeoutError(message)
        return _docker(*args, check=check, timeout=timeout)

    def _inspect_owned(self):
        result = self._command("inspect", self.container_id or self.name, check=False)
        if result.returncode:
            if any(
                text in result.stderr.lower()
                for text in ("no such object", "no such container")
            ):
                return None
            raise RuntimeError(result.stderr)
        data = json.loads(result.stdout)[0]
        if (
            data["Name"] != "/" + self.name
            or data["Config"].get("Labels", {}).get(_OWNER_LABEL) != self.owner
            or (self.container_id is not None and data["Id"] != self.container_id)
        ):
            message = "refusing to operate on a container without fixture ownership"
            raise RuntimeError(message)
        return data

    def start(self):
        self._startup_deadline = time.monotonic() + self.startup_timeout
        try:
            self._start()
        finally:
            self._startup_deadline = None

    def _start(self):
        self.record()
        # Keep uncertainty even if cancellation supersedes a create timeout
        # while the Docker adapter is returning. Cleanup must confirm ownership.
        self.report["creation_indeterminate"] = True
        created = self._command(
            "create",
            "--pull=never",
            "--name",
            self.name,
            "--label",
            f"{_OWNER_LABEL}={self.owner}",
            "--publish",
            "127.0.0.1::27017",
            "--",
            self.image,
            "mongod",
            "--replSet",
            "mongoeco",
            "--bind_ip_all",
            "--oplogSize",
            "128",
            "--setParameter",
            "ttlMonitorSleepSecs=1",
        )
        self.container_id = created.stdout.strip()
        self.report["creation_indeterminate"] = False
        self.report["container_id"] = self.container_id
        self._inspect_owned()
        self._command("start", self.container_id)
        data = self._inspect_owned()
        binding = data["NetworkSettings"]["Ports"]["27017/tcp"][0]
        if binding["HostIp"] != "127.0.0.1":
            message = "fixture must bind only to host loopback"
            raise RuntimeError(message)
        self.uri = (
            f"mongodb://127.0.0.1:{int(binding['HostPort'])}/"
            "?directConnection=true&serverSelectionTimeoutMS=3000"
        )
        self.report["uri"] = self.uri
        self._initialize()
        self.report["runtime"] = inspect_server_runtime(
            self.uri, self.target, require_replica_set=True
        )
        self.report["status"] = "ready"
        self.record()

    def _initialize(self):
        from pymongo import MongoClient  # noqa: PLC0415 - optional native dependency
        from pymongo.errors import PyMongoError  # noqa: PLC0415

        deadline = self._startup_deadline
        with MongoClient(
            self.uri,
            connectTimeoutMS=1000,
            socketTimeoutMS=3000,
            serverSelectionTimeoutMS=1000,
        ) as client:
            self._wait(lambda: client.admin.command("ping"), deadline, PyMongoError)
            client.admin.command(
                {
                    "replSetInitiate": {
                        "_id": "mongoeco",
                        "members": [{"_id": 0, "host": "127.0.0.1:27017"}],
                    }
                }
            )
            self._wait(
                lambda: client.admin.command("hello").get("isWritablePrimary"),
                deadline,
                PyMongoError,
            )
            client.admin.command(
                {
                    "setFeatureCompatibilityVersion": self.target,
                    "confirm": True,
                }
            )
            self.report["server_options"] = client.admin.command("getCmdLineOpts")[
                "parsed"
            ]

    @staticmethod
    def _wait(probe, deadline, transient_error):
        while True:
            try:
                if probe():
                    return
            except transient_error:
                pass
            if time.monotonic() >= deadline:
                message = "replica-set startup deadline exceeded"
                raise TimeoutError(message)
            time.sleep(0.1)

    def _remove_volumes(self):
        for name in self.report.get("anonymous_volumes", []):
            result = _docker("volume", "inspect", name, check=False)
            if result.returncode:
                if "no such volume" in result.stderr.lower():
                    continue
                raise RuntimeError(result.stderr)
            # A named/external volume is never mounted by this fixture. Docker
            # also refuses to remove any volume that is now used elsewhere.
            _docker("volume", "rm", name)

    def close(self):
        data = self._inspect_owned()
        if data is None:
            if self.report.get("creation_indeterminate"):
                message = (
                    "cannot confirm cleanup after a timed-out Docker create request"
                )
                raise RuntimeError(message)
            self._remove_volumes()
            self.report["cleanup"] = {
                "removed": True,
                "already_absent": True,
                "volumes_removed": True,
            }
            return
        container = data["Id"]
        self.report["creation_indeterminate"] = False
        self.report["anonymous_volumes"] = [
            mount["Name"]
            for mount in data.get("Mounts", [])
            if mount.get("Type") == "volume"
        ]
        try:
            logs = _docker("logs", container, check=False)
            (self.artifacts / "mongod.log").write_text(logs.stdout + logs.stderr)
        finally:
            _docker("rm", "--force", "--volumes", container)
        if self._inspect_owned() is not None:
            message = "fixture container survived cleanup"
            raise RuntimeError(message)
        self._remove_volumes()
        self.report["cleanup"] = {
            "removed": True,
            "container_id": container,
            "volumes_removed": True,
        }


def _signal_group(process, signum):
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signum)


def _stop_payload(process):
    with _defer_cancellation():
        # A shell may exit before its children; clean the owned group as well.
        _signal_group(process, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        finally:
            _signal_group(process, signal.SIGKILL)
            process.wait(timeout=5)


def run_payload(command, uri, *, timeout):
    process = None
    try:
        # Retain ownership before delivering a queued cancellation, including
        # one dispatched while Popen is still returning the child's handle.
        with _defer_cancellation():
            process = subprocess.Popen(  # noqa: S603 - explicit caller argv, no shell
                command,
                env=dict(os.environ, MONGOECO_REAL_MONGODB_URI=uri),
                start_new_session=True,
            )
        return process.wait(timeout=timeout)
    finally:
        if process is not None:
            _stop_payload(process)


class _FixtureCancelledError(Exception):
    def __init__(self, signum):
        self.signum = signum
        super().__init__(f"fixture interrupted by signal {signum}")


def _cancel(signum, _frame):
    if _CANCELLATION.depth:
        if _CANCELLATION.pending is None:
            _CANCELLATION.pending = signum
        return
    raise _FixtureCancelledError(signum)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--target", choices=("7.0", "8.0", "9.0"), required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--startup-timeout", type=float, default=90)
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or any(
        not math.isfinite(value) or value <= 0
        for value in (args.timeout, args.startup_timeout)
    ):
        parser.error("a command and positive startup/payload timeouts are required")
    fixture = ReplicaSetFixture(
        args.image, args.target, args.artifacts, startup_timeout=args.startup_timeout
    )
    handlers = {
        signum: signal.signal(signum, _cancel)
        for signum in (signal.SIGTERM, signal.SIGINT)
    }
    status = 1
    try:
        fixture.start()
        fixture.report["payload"] = {"command": command}
        status = run_payload(command, fixture.uri, timeout=args.timeout)
        fixture.report["payload"]["exit_code"] = status
        fixture.report["status"] = "succeeded" if status == 0 else "failed"
    except _FixtureCancelledError as error:
        status = 128 + error.signum
        fixture.report.update(status="cancelled", error=str(error))
    except Exception as error:
        fixture.report.update(
            status="failed", error=repr(error), traceback=traceback.format_exc()
        )
        sys.stderr.write(f"replica-set fixture failed: {error!r}\n")
    finally:
        for signum in handlers:
            signal.signal(signum, signal.SIG_IGN)
        _CANCELLATION.pending = None
        try:
            fixture.close()
        except Exception as error:
            status = 1
            fixture.report.update(
                status="failed", cleanup={"removed": False, "error": repr(error)}
            )
            sys.stderr.write(f"replica-set cleanup failed: {error!r}\n")
        finally:
            try:
                fixture.record()
            finally:
                for signum, handler in handlers.items():
                    signal.signal(signum, handler)
    return status if status >= 0 else 128 - status


if __name__ == "__main__":
    raise SystemExit(main())
