"""Selection coverage, owned cleanup and real payload process-group behavior."""

import json
import os
import signal
import subprocess
import sys
import time
import unittest

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scripts import with_mongodb_replica_set as replica_fixture
from scripts.capture_differential_replay_golden import main as capture_main
from scripts.run_mongodb_real_differential import inspect_server_runtime, main
from scripts.with_mongodb_replica_set import (
    MongoDBFixture,
    main as fixture_main,
    run_payload,
)

from tests.differential.cases import RealParityCase
from tests.differential.runner import available_case_names, build_suite, select_cases


IMAGE = "mongo:7.0.43-jammy@sha256:" + "a" * 64


def test_fixture_refuses_an_existing_receipt_before_docker_or_overwrite(tmp_path):
    report = tmp_path / "fixture.json"
    previous = b'{"owner":"previous","container_id":"retained-evidence"}\n'
    report.write_bytes(previous)
    with (
        patch.object(replica_fixture, "_docker") as docker,
        pytest.raises(FileExistsError, match="use a new artifacts directory"),
    ):
        MongoDBFixture(IMAGE, "7.0", tmp_path)
    docker.assert_not_called()
    assert report.read_bytes() == previous


def test_fixture_claim_is_exclusive_before_the_first_report(tmp_path):
    owner = MongoDBFixture(IMAGE, "7.0", tmp_path)
    with (
        patch.object(replica_fixture, "_docker") as docker,
        pytest.raises(FileExistsError),
    ):
        MongoDBFixture(IMAGE, "7.0", tmp_path)
    docker.assert_not_called()
    owner.record()
    owner.report["status"] = "succeeded"
    owner.record()
    assert json.loads((tmp_path / "fixture.json").read_text())["owner"] == owner.owner


@pytest.mark.parametrize("topology", ["replica-set", "standalone"])
def test_owned_fixture_passes_only_the_requested_topology_to_mongod(tmp_path, topology):
    fixture = MongoDBFixture(IMAGE, "7.0", tmp_path, topology=topology)
    command = MagicMock(return_value=subprocess.CompletedProcess([], 0, "owned", ""))
    inspected = {
        "NetworkSettings": {
            "Ports": {"27017/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49231"}]}
        }
    }
    with (
        patch.object(fixture, "_command", command),
        patch.object(fixture, "_inspect_owned", return_value=inspected),
        patch.object(fixture, "_initialize"),
        patch.object(
            replica_fixture, "inspect_server_runtime", return_value={}
        ) as inspect_runtime,
    ):
        fixture._start()
    arguments = command.call_args_list[0].args
    assert ("--replSet" in arguments) == (topology == "replica-set")
    assert ("--oplogSize" in arguments) == (topology == "replica-set")
    assert fixture.report["topology"] == topology
    inspect_runtime.assert_called_once_with(
        fixture.uri,
        "7.0",
        require_replica_set=topology == "replica-set",
        **({"require_standalone": True} if topology == "standalone" else {}),
    )


@pytest.mark.parametrize(
    "hello",
    [
        {"setName": "unexpected", "isWritablePrimary": True},
        {"msg": "isdbgrid", "isWritablePrimary": True},
        {"isWritablePrimary": False},
    ],
)
def test_standalone_initialization_rejects_another_topology_before_setting_fcv(
    tmp_path, hello
):
    fixture = MongoDBFixture(IMAGE, "7.0", tmp_path, topology="standalone")
    fixture.uri = "mongodb://owned"
    client = MagicMock()
    client.admin.command.side_effect = [
        {"ok": 1},
        hello,
    ]
    with patch("pymongo.MongoClient") as factory:
        factory.return_value.__enter__.return_value = client
        with pytest.raises(RuntimeError, match="standalone fixture unexpectedly"):
            fixture._initialize()
    assert [call.args[0] for call in client.admin.command.call_args_list] == [
        "ping",
        "hello",
    ]


@pytest.mark.parametrize(
    "hello",
    [
        {"setName": "rs", "isWritablePrimary": True},
        {"msg": "isdbgrid", "isWritablePrimary": True},
        {"isWritablePrimary": False},
    ],
)
def test_capture_preflight_rejects_other_topologies_for_standalone_oracles(hello):
    client = MagicMock()
    client.__enter__.return_value = client
    client.server_info.return_value = {"versionArray": [7, 0, 43]}
    client.admin.command.side_effect = lambda value: (
        hello
        if value == "hello"
        else {"featureCompatibilityVersion": {"version": "7.0"}}
    )
    with (
        patch("pymongo.MongoClient", return_value=client),
        pytest.raises(RuntimeError, match="requires a writable standalone"),
    ):
        inspect_server_runtime("mongodb://unused", "7.0", require_standalone=True)


def test_incompatible_topology_requirements_fail_before_connecting():
    with (
        patch("pymongo.MongoClient") as factory,
        pytest.raises(RuntimeError, match="partitions separately"),
    ):
        inspect_server_runtime(
            "mongodb://unused",
            "7.0",
            require_standalone=True,
            require_replica_set=True,
        )
    factory.assert_not_called()


@pytest.mark.parametrize("version", [(7, 0), (8, 0), (9, 0)])
def test_partitions_cover_exactly_the_filtered_catalog_and_load_every_case(version):
    all_cases = set(available_case_names(version))
    standalone = set(available_case_names(version, partition="standalone"))
    replica = set(available_case_names(version, partition="replica-set"))
    assert all_cases == standalone | replica
    assert not standalone & replica
    assert "semantic_ttl_publication_deletes" in replica
    assert any(name.startswith("semantic_update_entrypoint") for name in replica)
    for partition in ("all", "standalone", "replica-set"):
        names = available_case_names(version, "*watch*,*ttl*", partition=partition)
        if names:
            suite = build_suite(
                ".".join(map(str, version)), "*watch*,*ttl*", partition=partition
            )
            assert suite.countTestCases() == len(names)
        else:
            with pytest.raises(ValueError, match="matched no differential cases"):
                build_suite(
                    ".".join(map(str, version)), "*watch*,*ttl*", partition=partition
                )


def test_list_modes_apply_filter_and_partition_without_connecting(capsys, monkeypatch):
    monkeypatch.delenv("MONGOECO_REAL_MONGODB_URI", raising=False)
    assert (
        main(
            [
                "runner",
                "7.0",
                "semantic_ttl_*",
                "--list-cases",
                "--partition",
                "replica-set",
            ]
        )
        == 0
    )
    assert capsys.readouterr().out.splitlines() == ["semantic_ttl_publication_deletes"]
    assert (
        main(
            [
                "runner",
                "7.0",
                "semantic_ttl_*",
                "--list-corpora",
                "--partition",
                "replica-set",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out) == [
        {"case_set": "ttl-publication", "fixture_key": "ttl_publication"}
    ]
    assert not select_cases((7, 0), "semantic_ttl_*", partition="standalone")
    with pytest.raises(ValueError, match="matched no differential cases"):
        build_suite("7.0", "semantic_ttl_*", partition="standalone")


def test_runner_rejects_a_partially_missing_suite():
    names = available_case_names((7, 0), "semantic_watch_*", partition="replica-set")
    assert len(names) > 1
    test = unittest.FunctionTestCase(lambda: None)
    test._testMethodName = f"test_{names[0]}_matches_real_mongodb"
    with (
        patch(
            "tests.differential.runner.unittest.defaultTestLoader.loadTestsFromName",
            return_value=unittest.TestSuite([test]),
        ),
        pytest.raises(ValueError, match="did not load every selected case"),
    ):
        build_suite("7.0", "semantic_watch_*", partition="replica-set")


@pytest.mark.parametrize(
    "hello",
    [{"isWritablePrimary": True}, {"setName": "rs", "isWritablePrimary": False}],
)
def test_preflight_requires_a_writable_replica_set_when_requested(hello):
    client = MagicMock()
    client.__enter__.return_value = client
    client.server_info.return_value = {"versionArray": [7, 0, 43]}
    client.admin.command.side_effect = lambda value: (
        hello
        if value == "hello"
        else {"featureCompatibilityVersion": {"version": "7.0"}}
    )
    with (
        patch("pymongo.MongoClient", return_value=client),
        pytest.raises(RuntimeError, match="writable replica-set primary"),
    ):
        inspect_server_runtime("mongodb://unused", "7.0", require_replica_set=True)


@pytest.mark.parametrize(
    ["case_name", "partition", "standalone"],
    [
        ("semantic_ttl_publication_deletes", "replica-set", False),
        ("semantic_document_factory_trace", "standalone", True),
    ],
)
def test_report_uses_the_filtered_selection_and_requires_its_topology(
    tmp_path, monkeypatch, case_name, partition, standalone
):
    monkeypatch.setenv("MONGOECO_REAL_MONGODB_URI", "mongodb://unused")
    report = tmp_path / "report.json"
    with (
        patch(
            "scripts.run_mongodb_real_differential.build_suite",
            return_value=unittest.TestSuite([unittest.FunctionTestCase(lambda: None)]),
        ),
        patch(
            "scripts.run_mongodb_real_differential.inspect_server_runtime",
            return_value={},
        ) as preflight,
    ):
        assert (
            main(
                [
                    "runner",
                    "7.0",
                    case_name,
                    "--partition",
                    partition,
                    "--json-report",
                    str(report),
                ]
            )
            == 0
        )
    data = json.loads(report.read_text())
    assert [row["name"] for row in data["cases"]] == [case_name]
    assert data["partition"] == partition
    preflight.assert_called_once_with(
        "mongodb://unused",
        "7.0",
        require_replica_set=not standalone,
        **({"require_standalone": True} if standalone else {}),
    )


def test_topology_metadata_does_not_rewrite_historical_reproduction_manifests():
    case = RealParityCase("case", [], lambda value: value, requires_replica_set=True)
    assert case.to_manifest() == {
        "name": "case",
        "min_version": [7, 0],
        "seed_documents": [],
        "selector": "case",
    }


@pytest.mark.parametrize(
    "stderr", ["Error: No such object: owned", "error: no such container: owned"]
)
def test_cleanup_distinguishes_absence_from_docker_failure(tmp_path, stderr):
    fixture = MongoDBFixture(IMAGE, "7.0", tmp_path)
    with patch(
        "scripts.with_mongodb_replica_set._docker",
        return_value=subprocess.CompletedProcess([], 1, "", stderr),
    ) as docker:
        fixture.close()
    assert fixture.report["cleanup"]["already_absent"]
    assert docker.call_count == 1
    with (
        patch(
            "scripts.with_mongodb_replica_set._docker",
            return_value=subprocess.CompletedProcess([], 1, "", "permission denied"),
        ),
        pytest.raises(RuntimeError, match="permission denied"),
    ):
        fixture.close()


def test_cleanup_refuses_a_container_without_the_owned_identity(tmp_path):
    fixture = MongoDBFixture(IMAGE, "7.0", tmp_path)
    other = [{"Id": "borrowed", "Name": "/" + fixture.name, "Config": {"Labels": {}}}]
    with (
        patch(
            "scripts.with_mongodb_replica_set._docker",
            return_value=subprocess.CompletedProcess([], 0, json.dumps(other), ""),
        ) as docker,
        pytest.raises(RuntimeError, match="without fixture ownership"),
    ):
        fixture.close()
    assert docker.call_count == 1


@pytest.mark.skipif(os.name != "posix", reason="fixture runs on POSIX CI workers")
@pytest.mark.parametrize("wait", [False, True])
def test_payload_completion_and_timeout_kill_remaining_children(tmp_path, wait):
    marker = tmp_path / "child.pid"
    child = (
        "import os,signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        f"Path({str(marker)!r}).write_text(str(os.getpid())); time.sleep(300)"
    )
    leader = (
        "import subprocess,sys,time; from pathlib import Path; "
        f"subprocess.Popen([sys.executable,'-c',{child!r}]); "
        f"marker=Path({str(marker)!r}); "
        "deadline=time.monotonic()+5\n"
        "while not marker.exists() and time.monotonic()<deadline: time.sleep(.01)\n"
        + ("time.sleep(300)" if wait else "assert marker.exists()")
    )
    if wait:
        with pytest.raises(subprocess.TimeoutExpired):
            run_payload([sys.executable, "-c", leader], "mongodb://unused", timeout=2)
    else:
        assert (
            run_payload([sys.executable, "-c", leader], "mongodb://unused", timeout=5)
            == 0
        )
    pid = int(marker.read_text())
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        state = subprocess.run(  # noqa: S603 - fixed ps invocation for the owned PID
            ["/bin/ps", "-o", "stat=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        if not state or state.startswith("Z"):
            break
        time.sleep(0.01)
    else:
        os.kill(pid, signal.SIGKILL)
        pytest.fail("payload child survived fixture cleanup")


@pytest.mark.parametrize("failure", ["startup", "payload"])
def test_main_records_failures_and_attempts_owned_cleanup(tmp_path, failure):
    closed = []

    def start(fixture):
        if failure == "startup":
            message = "startup failed"
            raise TimeoutError(message)
        fixture.uri = "mongodb://owned"

    def close(fixture):
        closed.append(fixture.name)
        fixture.report["cleanup"] = {"removed": True}

    with (
        patch.object(MongoDBFixture, "start", start),
        patch.object(MongoDBFixture, "close", close),
        patch(
            "scripts.with_mongodb_replica_set.run_payload",
            side_effect=subprocess.TimeoutExpired("owned", 1),
        ),
    ):
        assert (
            fixture_main(
                [
                    "--image",
                    IMAGE,
                    "--target",
                    "7.0",
                    "--artifacts",
                    str(tmp_path),
                    "--",
                    sys.executable,
                    "-c",
                    "pass",
                ]
            )
            == 1
        )
    report = json.loads((tmp_path / "fixture.json").read_text())
    assert report["status"] == "failed"
    assert report["cleanup"]["removed"]
    assert closed == [report["name"]]


def test_workflow_reuses_pinned_matrix_and_checks_partition_coverage(tmp_path):
    yaml = pytest.importorskip("yaml")
    document = yaml.safe_load(
        Path(".github/workflows/mongodb-differential.yml").read_text()
    )
    job = document["jobs"]["differential"]
    assert job["services"]["mongodb"]["image"] == "${{ matrix.image }}"
    assert all(
        "@sha256:" in row["image"] for row in job["strategy"]["matrix"]["include"]
    )
    steps = {step["name"]: step for step in job["steps"]}
    rs = steps["Verify replica-set differential partition and captures"]
    assert rs["env"]["MONGOECO_RS_IMAGE"] == "${{ matrix.image }}"
    assert "scripts/with_mongodb_replica_set.py" in rs["run"]
    assert "--list-corpora" in rs["run"]
    assert '"--check"' in rs["run"]
    assert '"pythonpath="' in rs["run"]
    selection = steps["Resolve and verify topology partitions"]["run"]
    assert (
        'assert selected["all"] == selected["standalone"] | selected["replica-set"]'
        in selection
    )
    assert 'assert selected["all"]' in selection
    for index, step in enumerate(job["steps"]):
        if "run" in step:
            script = tmp_path / f"step-{index}.sh"
            script.write_text(step["run"])
            subprocess.run(["/bin/bash", "-n", str(script)], check=True)  # noqa: S603


def test_cleanup_failure_cannot_turn_a_successful_payload_green(tmp_path):
    def start(fixture):
        fixture.uri = "mongodb://owned"

    message = "owned resource could not be removed"
    with (
        patch.object(MongoDBFixture, "start", start),
        patch.object(MongoDBFixture, "close", side_effect=RuntimeError(message)),
        patch("scripts.with_mongodb_replica_set.run_payload", return_value=0),
    ):
        assert (
            fixture_main(
                [
                    "--image",
                    IMAGE,
                    "--target",
                    "7.0",
                    "--artifacts",
                    str(tmp_path),
                    "--",
                    sys.executable,
                    "-c",
                    "pass",
                ]
            )
            == 1
        )
    report = json.loads((tmp_path / "fixture.json").read_text())
    assert report["payload"]["exit_code"] == 0
    assert report["status"] == "failed"
    assert report["cleanup"]["removed"] is False


@pytest.mark.parametrize("flag", ["--timeout", "--startup-timeout"])
@pytest.mark.parametrize("value", ["nan", "inf", "0", "-1"])
def test_unbounded_or_nonpositive_timeouts_are_rejected_before_resource_creation(
    tmp_path, flag, value
):
    with pytest.raises(SystemExit) as error:
        fixture_main(
            [
                "--image",
                IMAGE,
                "--target",
                "7.0",
                "--artifacts",
                str(tmp_path),
                flag,
                value,
                "--",
                sys.executable,
                "-c",
                "pass",
            ]
        )
    assert error.value.code == 2  # noqa: PLR2004 - argparse usage status
    assert not (tmp_path / "fixture.json").exists()


@pytest.mark.parametrize(
    ["case_set", "required", "standalone"],
    [
        ("ttl-publication", True, False),
        ("type-query", False, False),
        ("document-factory", False, True),
    ],
)
def test_capture_requires_the_declared_topology_before_writing_a_reference(
    tmp_path, monkeypatch, case_set, required, standalone
):
    monkeypatch.setenv("MONGOECO_REAL_MONGODB_URI", "mongodb://unused")
    output = tmp_path / "native.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["capture", "--target", "7.0", "--case-set", case_set, "--output", str(output)],
    )
    message = "preflight rejects the server"
    with (
        patch(
            "scripts.capture_differential_replay_golden.inspect_server_runtime",
            side_effect=RuntimeError(message),
        ) as preflight,
        pytest.raises(RuntimeError, match=message),
    ):
        capture_main()
    preflight.assert_called_once_with(
        "mongodb://unused",
        "7.0",
        require_replica_set=required,
        **({"require_standalone": True} if standalone else {}),
    )
    assert not output.exists()


def test_queued_python_signal_is_delivered_only_after_the_critical_request_finishes():
    completed = []

    def requests():
        with replica_fixture._defer_cancellation():
            with replica_fixture._defer_cancellation():
                replica_fixture._cancel(signal.SIGTERM, None)
                completed.append("inner request finished")
            completed.append("outer ownership recorded")

    with pytest.raises(replica_fixture._FixtureCancelledError) as error:
        requests()
    assert error.value.signum == signal.SIGTERM
    assert completed == ["inner request finished", "outer ownership recorded"]
    assert replica_fixture._CANCELLATION.depth == 0
    assert replica_fixture._CANCELLATION.pending is None


def test_indeterminate_creation_is_not_reported_as_successful_cleanup(tmp_path):
    fixture = MongoDBFixture(IMAGE, "7.0", tmp_path)
    fixture.report["creation_indeterminate"] = True
    with (
        patch.object(fixture, "_inspect_owned", return_value=None),
        pytest.raises(RuntimeError, match="cannot confirm cleanup"),
    ):
        fixture.close()
    assert not fixture.report["cleanup"].get("removed")


def test_docker_request_finishes_before_a_queued_python_signal_is_delivered():
    completed = []

    def request(*_args, **_kwargs):
        replica_fixture._cancel(signal.SIGTERM, None)
        completed.append("Docker response received")
        return subprocess.CompletedProcess([], 0, "owned", "")

    with (
        patch(
            "scripts.with_mongodb_replica_set.shutil.which",
            return_value="/owned/docker",
        ),
        patch("scripts.with_mongodb_replica_set.subprocess.run", side_effect=request),
        pytest.raises(replica_fixture._FixtureCancelledError),
    ):
        replica_fixture._docker("create", "owned")
    assert completed == ["Docker response received"]
    assert replica_fixture._CANCELLATION.depth == 0
    assert replica_fixture._CANCELLATION.pending is None


def test_cancellation_during_payload_creation_cleans_the_returned_process():
    process = MagicMock()

    def spawn(*_args, **_kwargs):
        replica_fixture._cancel(signal.SIGTERM, None)
        return process

    with (
        patch("scripts.with_mongodb_replica_set.subprocess.Popen", side_effect=spawn),
        patch("scripts.with_mongodb_replica_set._stop_payload") as stop,
        pytest.raises(replica_fixture._FixtureCancelledError),
    ):
        run_payload(["owned"], "mongodb://owned", timeout=1)
    stop.assert_called_once_with(process)
    process.wait.assert_not_called()
    assert replica_fixture._CANCELLATION.depth == 0
    assert replica_fixture._CANCELLATION.pending is None


@pytest.mark.parametrize(
    "error",
    [
        subprocess.TimeoutExpired("docker create", 1),
        replica_fixture._FixtureCancelledError(signal.SIGTERM),
    ],
)
def test_interrupted_create_keeps_cleanup_indeterminate_until_inspected(
    tmp_path, error
):
    fixture = MongoDBFixture(IMAGE, "7.0", tmp_path)
    with (
        patch.object(fixture, "_command", side_effect=error),
        pytest.raises(type(error)),
    ):
        fixture.start()
    assert fixture.report["creation_indeterminate"]
    with (
        patch.object(fixture, "_inspect_owned", return_value=None),
        pytest.raises(RuntimeError, match="cannot confirm cleanup"),
    ):
        fixture.close()
