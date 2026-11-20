"""Container entrypoints for local execution and storage checks."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import tempfile
import threading

from pixguard_ai.application.generate_events import GenerationConfig, generate_events
from pixguard_ai.application.capture_events import CaptureRequest, capture_events
from pixguard_ai.infrastructure.kafka.capture import KafkaCaptureSource
from pixguard_ai.infrastructure.synthetic.generator import SyntheticEventSource
from pixguard_ai.infrastructure.synthetic.jsonl import JsonlEventWriter


def data_directory() -> Path:
    """Return the configured shared storage directory."""
    return Path(os.environ.get("PIXGUARD_DATA_DIR", "/data/pixguard"))


def check_storage() -> None:
    """Verify read/write access without leaving probe files in shared data."""
    directory = data_directory()
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(mode="w+", dir=directory) as probe:
        probe.write("pixguard-storage-probe")
        probe.seek(0)
        if probe.read() != "pixguard-storage-probe":
            raise OSError("Shared storage probe could not read its contents")
    print(f"Shared storage is readable and writable: {directory}")


def idle() -> None:
    """Keep the service available until Docker requests a graceful stop."""
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stopped.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stopped.set())
    check_storage()
    print("PixGuard execution shell ready; all payment data is synthetic.")
    stopped.wait()


def dashboard() -> None:
    """Launch the UI when available, otherwise run the reserved service shell."""
    module = importlib.util.find_spec("pixguard_ai.interfaces.dashboard")
    if module is None or module.origin is None:
        print("Dashboard pending UI-001; running the reserved container shell.")
        idle()
        return
    os.execvp(
        "streamlit",
        ["streamlit", "run", module.origin, "--server.address=0.0.0.0",
         "--server.port=8501", "--browser.gatherUsageStats=false"],
    )


def main() -> None:
    """Parse container commands and compose concrete generation adapters."""
    parser = argparse.ArgumentParser(description="PixGuard AI container tools")
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in ("idle", "check-storage", "dashboard"):
        subcommands.add_parser(command)
    generator = subcommands.add_parser("generate", help="Generate fictional payment events offline")
    generator.add_argument("--seed", type=int, default=os.environ.get("PIXGUARD_SEED", "42"))
    generator.add_argument("--count", type=int, default=os.environ.get("PIXGUARD_EVENT_COUNT", "100000"))
    generator.add_argument("--output", type=Path, default=data_directory() / "events.jsonl")
    publisher = subcommands.add_parser("publish", help="Publish a validated synthetic batch to Kafka")
    publisher.add_argument("--input", type=Path, default=data_directory() / "events.jsonl")
    publisher.add_argument("--batch-id", required=True)
    publisher.add_argument("--output-root", type=Path, default=data_directory() / "publications")
    publisher.add_argument("--timeout", type=float, default=60.0)
    capture = subcommands.add_parser("capture", help="Capture a fixed Kafka offset range")
    capture.add_argument("--capture-id", required=True)
    capture.add_argument("--topic", default=os.environ.get("PIXGUARD_KAFKA_TOPIC", "pixguard.payment_event_v1"))
    capture.add_argument("--partition", type=int, default=0)
    capture.add_argument("--start", type=int, required=True)
    capture.add_argument("--end", type=int)
    capture.add_argument("--output-root", type=Path, default=data_directory() / "captures")
    arguments = parser.parse_args()
    if arguments.command == "capture":
        from pixguard_ai.infrastructure.kafka.capture_storage import CaptureStorage

        try:
            bootstrap = os.environ.get("PIXGUARD_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
            source = KafkaCaptureSource(bootstrap, arguments.capture_id, arguments.topic, arguments.partition)
            cluster_id = source.identity()
            request = CaptureRequest(
                capture_id=arguments.capture_id,
                cluster_id=cluster_id,
                topic=arguments.topic,
                partition=arguments.partition,
                start=arguments.start,
                end=arguments.end,
                data_root=arguments.output_root,
            )
            manifest = capture_events(request, source, CaptureStorage(arguments.output_root))
        except KeyboardInterrupt:
            parser.exit(130, "Capture interrupted; persisted records and manifest can be resumed.\n")
        except Exception as error:
            parser.exit(2, f"Capture failed ({type(error).__name__}); inspect its manifest.\n")
        finally:
            if "source" in locals():
                source.close()
        print(json.dumps(manifest, sort_keys=True))
        return
    if arguments.command == "publish":
        from pixguard_ai.application.publish_events import publish_events
        from pixguard_ai.infrastructure.kafka.producer import KafkaPublisher
        from pixguard_ai.infrastructure.kafka.storage import FilePublicationStore, JsonlPublicationSource

        previous_handler = signal.getsignal(signal.SIGTERM)

        def stop_publication(_signum: int, _frame: object) -> None:
            """Leave interrupted delivery explicitly uncertain on graceful stop."""
            raise KeyboardInterrupt

        try:
            store = FilePublicationStore(arguments.output_root)
            source = JsonlPublicationSource(arguments.input, store)
            kafka = KafkaPublisher(
                os.environ.get("PIXGUARD_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                os.environ.get("PIXGUARD_KAFKA_TOPIC", "pixguard.payment_event_v1"),
                arguments.timeout,
            )
            signal.signal(signal.SIGTERM, stop_publication)
            manifest = publish_events(arguments.batch_id, source, kafka, store, arguments.timeout)
        except KeyboardInterrupt:
            parser.exit(130, "Publication interrupted; inspect its manifest.\n")
        except Exception as error:
            parser.exit(2, f"Publication failed ({type(error).__name__}); inspect its manifest.\n")
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
        print(json.dumps(manifest, sort_keys=True))
        return
    if arguments.command == "generate":
        previous_handler = signal.getsignal(signal.SIGTERM)

        def interrupt(_signum: int, _frame: object) -> None:
            """Allow Docker stop to persist the observed interrupted progress."""
            raise KeyboardInterrupt

        try:
            config = GenerationConfig(seed=arguments.seed, count=arguments.count)
            writer = JsonlEventWriter(arguments.output)
            signal.signal(signal.SIGTERM, interrupt)
            manifest = generate_events(config, SyntheticEventSource(), writer)
        except (ValueError, OSError) as error:
            parser.exit(2, f"Generation failed: {error}\n")
        except KeyboardInterrupt:
            parser.exit(130, "Generation interrupted; inspect the progress manifest.\n")
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
        print(json.dumps(manifest, sort_keys=True))
        return
    commands = {"idle": idle, "check-storage": check_storage, "dashboard": dashboard}
    commands[arguments.command]()


if __name__ == "__main__":
    main()
