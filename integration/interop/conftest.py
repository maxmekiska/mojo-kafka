"""Fixtures for the cross-client interop suite.

Builds the Mojo peers once per session -- `mojo build` costs a few seconds and
the matrix invokes each peer many times -- then exposes every client behind one
`Peer` interface so a test can name a producer and a consumer without caring
which language implements either.
"""

import itertools
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fixtures import from_field, from_headers  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
PEERS_DIR = Path(__file__).resolve().parent / "peers"

BOOTSTRAP = os.environ.get("MOJO_KAFKA_BOOTSTRAP", "localhost:9092")

# Long enough to cover a cold JIT and a slow broker, short enough that a hung
# peer fails the run instead of the CI job's global timeout.
PEER_TIMEOUT_S = 120


# --------------------------------------------------------------------------
# Building the Mojo peers
# --------------------------------------------------------------------------


def _mojo_argv() -> list[str]:
    """How to invoke `mojo`, whether or not we are already inside a pixi env.

    Running under `pixi run -e interop` puts `mojo` straight on PATH, so
    shelling back out through pixi would spawn a redundant nested environment.
    Outside one -- a bare `pytest` invocation -- fall back to pixi.
    """
    direct = shutil.which("mojo")
    if direct:
        return [direct]

    pixi = shutil.which("pixi") or str(Path.home() / ".pixi" / "bin" / "pixi")
    if not Path(pixi).exists():
        pytest.skip("neither mojo nor pixi on PATH; cannot build the Mojo peers")
    return [pixi, "run", "--", "mojo"]


@pytest.fixture(scope="session")
def mojo_peers(tmp_path_factory) -> dict[str, Path]:
    """Compile the Mojo peers once and hand back their binaries.

    Built rather than `mojo run`, so the matrix does not pay compilation on
    every cell.
    """
    out_dir = tmp_path_factory.mktemp("mojo-peers")
    built = {}
    for name in ("mojo_produce", "mojo_consume"):
        binary = out_dir / name
        result = subprocess.run(
            _mojo_argv() + [
                "build",
                "-I", "src",
                "-I", str(PEERS_DIR),
                str(PEERS_DIR / f"{name}.mojo"),
                "-o", str(binary),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=600,
        )
        if result.returncode != 0:
            pytest.fail(
                f"building {name} failed:\n{result.stdout}\n{result.stderr}"
            )
        built[name] = binary
    return built


# --------------------------------------------------------------------------
# Peers
# --------------------------------------------------------------------------


class PeerError(RuntimeError):
    """A peer process exited non-zero or produced unusable output."""


class Peer:
    """One client, able to act as producer or consumer.

    Subclasses only supply the argv; parsing the contract lines is shared, so
    every client is held to the same output format.
    """

    name: str

    def _run(self, argv: list[str]) -> str:
        result = subprocess.run(
            argv,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=PEER_TIMEOUT_S,
        )
        if result.returncode != 0:
            raise PeerError(
                f"{self.name} exited {result.returncode}\n"
                f"argv: {argv}\n"
                f"--- stdout ---\n{result.stdout}\n"
                f"--- stderr ---\n{result.stderr}"
            )
        return result.stdout

    def produce_argv(self, topic, fixture_path): ...

    def consume_argv(self, topic, group, count): ...

    def produce(self, topic: str, fixture_path: Path) -> int:
        out = self._run(self.produce_argv(topic, fixture_path))
        for line in out.splitlines():
            if line.startswith("PRODUCED"):
                return int(line.split()[1])
        raise PeerError(f"{self.name} produced no PRODUCED line:\n{out}")

    def consume(self, topic: str, group: str, count: int) -> list[tuple]:
        """Return the messages seen, as (partition, offset, key, value, headers)."""
        out = self._run(self.consume_argv(topic, group, count))
        messages = []
        for line in out.splitlines():
            if not line.startswith("MSG\t"):
                continue
            _, partition, offset, key, value, headers = line.split("\t")
            messages.append(
                (
                    int(partition),
                    int(offset),
                    from_field(key),
                    from_field(value),
                    from_headers(headers),
                )
            )
        return messages


class MojoPeer(Peer):
    name = "mojo"

    def __init__(self, binaries: dict[str, Path]):
        self._binaries = binaries

    def produce_argv(self, topic, fixture_path):
        return [
            str(self._binaries["mojo_produce"]),
            BOOTSTRAP, topic, str(fixture_path),
        ]

    def consume_argv(self, topic, group, count):
        return [
            str(self._binaries["mojo_consume"]),
            BOOTSTRAP, topic, group, str(count),
        ]


class PythonPeer(Peer):
    def __init__(self, client: str):
        self.name = client
        self._client = client

    def _base(self, topic):
        return [
            sys.executable, str(PEERS_DIR / "py_peer.py"),
            "--client", self._client,
            "--bootstrap", BOOTSTRAP,
            "--topic", topic,
        ]

    def produce_argv(self, topic, fixture_path):
        return self._base(topic) + [
            "--role", "produce", "--fixture", str(fixture_path),
        ]

    def consume_argv(self, topic, group, count):
        return self._base(topic) + [
            "--role", "consume", "--group", group, "--count", str(count),
        ]


@pytest.fixture(scope="session")
def peers(mojo_peers) -> dict[str, Peer]:
    return {
        "mojo": MojoPeer(mojo_peers),
        "confluent": PythonPeer("confluent"),
    }


# --------------------------------------------------------------------------
# Broker
# --------------------------------------------------------------------------


@pytest.fixture(scope="session")
def admin():
    """Admin client used only for test setup.

    Deliberately confluent-kafka rather than this package's `AdminClient`: the
    scaffolding that creates topics should not be the code under test, or a bug
    in our admin path shows up as every interop cell failing at once.
    """
    confluent = pytest.importorskip("confluent_kafka.admin")
    client = confluent.AdminClient({"bootstrap.servers": BOOTSTRAP})
    try:
        client.list_topics(timeout=15)
    except Exception as exc:
        # Reached when no broker is listening. Skipping rather than letting it
        # error keeps a missing broker to one legible line instead of a failure
        # per matrix cell, and says how to fix it.
        pytest.skip(
            f"no Kafka broker at {BOOTSTRAP} ({exc}); "
            "start one with `pixi run broker-up`"
        )
    return client


# Distinguishes topics created within the same process and clock tick.
_counter = itertools.count()


@pytest.fixture
def topic(admin, request):
    """A fresh single-partition topic, named after the test that asked for it.

    One partition keeps delivery order total, so the suite can assert on exact
    message order rather than just set membership.
    """
    from confluent_kafka.admin import NewTopic

    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in request.node.name)
    name = f"interop-{safe}-{os.getpid()}-{next(_counter)}"[:240]

    futures = admin.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1)]
    )
    futures[name].result(timeout=30)

    # Creation is acked before metadata propagates; a producer that races it
    # gets UNKNOWN_TOPIC_OR_PART. Wait for the topic to actually be visible.
    for _ in range(60):
        meta = admin.list_topics(timeout=10)
        if name in meta.topics and meta.topics[name].partitions:
            break
        time.sleep(0.25)
    else:
        pytest.fail(f"topic {name} never became visible")

    return name
