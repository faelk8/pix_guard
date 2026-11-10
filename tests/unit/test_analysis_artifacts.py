"""Filesystem boundaries and immutable publication contracts."""

from pathlib import Path

import pytest

from pixguard_ai.domain.analysis import RuleConfig
from pixguard_ai.infrastructure.local.analysis_artifacts import (
    AnalysisArtifacts, MAX_JSON_BYTES, atomic_json, file_record, protocol, read_json,
    safe_path, verify_records,
)


@pytest.mark.parametrize("relative", ["../outside", "/tmp/outside", "a/../../outside", ""])
def test_reject_unsafe_paths(tmp_path: Path, relative: str) -> None:
    with pytest.raises(ValueError):
        safe_path(tmp_path, relative, exists=False)


def test_reject_internal_symlink(tmp_path: Path) -> None:
    (tmp_path / "real").mkdir()
    (tmp_path / "link").symlink_to(tmp_path / "real")
    with pytest.raises(ValueError, match="symlink"):
        safe_path(tmp_path, "link/new", exists=False)


def test_corruption_and_json_bounds(tmp_path: Path) -> None:
    path = tmp_path / "data.json"
    atomic_json(path, {"value": 1})
    record = file_record(tmp_path, path)
    verify_records(tmp_path, [record])
    atomic_json(path, {"value": 2})
    with pytest.raises(ValueError, match="checksum"):
        verify_records(tmp_path, [record])
    path.write_text('{"x":1,"x":2}')
    with pytest.raises(ValueError, match="duplicate"):
        read_json(path)
    path.write_bytes(b" " * (MAX_JSON_BYTES + 1))
    with pytest.raises(ValueError, match="limit"):
        read_json(path)


def test_failure_and_immutable_run(tmp_path: Path) -> None:
    store = AnalysisArtifacts(tmp_path)
    with pytest.raises(RuntimeError):
        with store.reserve("failed"):
            raise RuntimeError("interrupted")
    manifest = read_json(tmp_path / "analysis-runs/failed/manifest.json")
    assert manifest["status"] == "failed"
    before = (tmp_path / "analysis-runs/failed/manifest.json").read_bytes()
    with pytest.raises(FileExistsError):
        with store.reserve("failed"):
            pass
    assert (tmp_path / "analysis-runs/failed/manifest.json").read_bytes() == before
    assert not (tmp_path / "analysis-locks/failed.lock").exists()
    with pytest.raises(ValueError):
        store.completed("failed")


def test_lock_excludes_second_writer(tmp_path: Path) -> None:
    store = AnalysisArtifacts(tmp_path)
    with store.reserve("one"):
        with pytest.raises(FileExistsError):
            with store.reserve("one"):
                pass


def test_protocol_explicit_limits() -> None:
    frozen = protocol(RuleConfig())
    assert frozen["blind"] is False and frozen["generalization_claim"] is False
    assert frozen["tuning"] == "none"
    assert frozen["pr_auc"]["numBins"] == 0
    assert frozen["rule"]["feature_columns"] == ["amount_decimal"]
