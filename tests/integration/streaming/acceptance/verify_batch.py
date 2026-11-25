"""Verify clean source/capture/snapshot conservation without float arithmetic."""

import argparse
import base64
from collections import Counter
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path


def digest(path: Path) -> str:
    """Hash the exact bytes without loading an entire capture."""
    result = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def canonical(value: object) -> bytes:
    """Normalize JSON objects for comparison across serialization changes."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def summarize(path: Path) -> tuple[Counter, Decimal, int]:
    """Return canonical event multiset and exact BRL sum."""
    hashes = Counter()
    total = Decimal("0")
    count = 0
    with path.open() as stream:
        for line in stream:
            event = json.loads(line)
            hashes[(event["event_id"], sha256(canonical(event)).hexdigest())] += 1
            total += Decimal(event["features"]["amount_brl"])
            count += 1
    return hashes, total, count


def main() -> None:
    """Fail on any mismatch and emit a compact machine-readable receipt."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--expected-count", type=int, default=100000)
    parser.add_argument("--expected-physical", type=int, default=100000)
    args = parser.parse_args()
    source, source_sum, source_count = summarize(args.source)
    exported, exported_sum, exported_count = summarize(args.snapshot / "events.jsonl")
    assert source_count == exported_count == args.expected_count
    assert len({key[0] for key in source}) == args.expected_count
    assert source == exported and source_sum == exported_sum
    manifest = json.loads((args.snapshot / "manifest.json").read_text())
    assert manifest["status"] == "completed"
    assert manifest["counts"] == {
        "physical": args.expected_physical, "exported": args.expected_count,
        "duplicates": args.expected_physical - args.expected_count,
        "transport_quarantine": 0, "contract_quarantine": 0,
    }
    for filename, field in (("events.jsonl", "events_sha256"), ("quarantine.jsonl", "quarantine_sha256")):
        assert digest(args.snapshot / filename) == manifest[field]
    capture_manifest = json.loads((args.capture / "manifest.json").read_text())
    segment = args.capture / "records.jsonl"
    assert capture_manifest["status"] == "completed"
    assert capture_manifest["count"] == args.expected_physical
    assert digest(segment) == capture_manifest["segment"]["sha256"] == manifest["capture"]["segment_sha256"]
    assert segment.stat().st_size == capture_manifest["segment"]["size_bytes"]
    physical = Counter()
    with segment.open() as stream:
        count = 0
        for ordinal, line in enumerate(stream):
            record = json.loads(line)
            checksum = record.pop("checksum_sha256")
            assert sha256(canonical(record)).hexdigest() == checksum
            assert record["offset"] == capture_manifest["start"] + ordinal
            assert record["topic"] == capture_manifest["topic"]
            assert record["partition"] == capture_manifest["partition"]
            event = json.loads(base64.b64decode(record["value_base64"], validate=True))
            physical[(event["event_id"], sha256(canonical(event)).hexdigest())] += 1
            count += 1
    assert count == args.expected_physical
    assert capture_manifest["end"] - capture_manifest["start"] == count
    assert args.expected_physical % args.expected_count == 0
    factor = args.expected_physical // args.expected_count
    assert physical == Counter({key: value * factor for key, value in source.items()})
    canonical_hash = sha256(canonical(sorted((key, value) for key, value in source.items()))).hexdigest()
    print(json.dumps({"status": "verified", "source_count": source_count,
        "physical_count": count, "exported_count": exported_count,
        "amount_brl": str(source_sum), "canonical_multiset_sha256": canonical_hash,
        "source_sha256": digest(args.source), "events_sha256": manifest["events_sha256"]}, sort_keys=True))


if __name__ == "__main__":
    main()
