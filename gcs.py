"""Bucket access and work claims.

A unit of work is claimed by creating gs://<bucket>/<work>/claims/<phase>/<unit> only if it does not exist
(if_generation_match=0), so that any number of processes on any number of VMs share the work without doing a unit
twice. A unit is done when its result object exists (written last). A claim older than `stale` seconds whose unit is
not done is taken over: deleted on its generation and created again, so that a dead process does not hold a unit.

use_gsutil() replaces exists, read_bytes, write_bytes, listing and claim by the same operations through the gsutil
command and its login, with the same preconditions, for a machine without application default credentials (the server
outside Google Cloud of fetch attempt 3). Every call starts a gsutil process (seconds of CPU), so it suits phases
with few, large units.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
from datetime import datetime, timezone

from google.api_core import exceptions as gexc
from google.cloud import storage

_local = threading.local()


def client() -> storage.Client:
    """A client of this process and thread: a client's connections shared by forked processes or by threads gave
    corrupted downloads (caught by the MD5 check of google-resumable-media)."""
    if getattr(_local, "pid", None) != os.getpid() or getattr(_local, "client", None) is None:
        _local.client = storage.Client()
        _local.pid = os.getpid()
    return _local.client


def split(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"not a gs:// URI: {uri}")
    b, _, name = uri[5:].partition("/")
    return b, name


def blob(uri: str) -> storage.Blob:
    b, name = split(uri)
    return client().bucket(b).blob(name)


def exists(uri: str) -> bool:
    return blob(uri).exists()


def read_bytes(uri: str) -> bytes:
    return blob(uri).download_as_bytes()


def write_bytes(uri: str, data: bytes, content_type: str = "application/octet-stream", only_new: bool = False):
    kw = {"if_generation_match": 0} if only_new else {}
    blob(uri).upload_from_string(data, content_type=content_type, **kw)


def upload(path: str, uri: str):
    blob(uri).upload_from_filename(path)


def download(uri: str, path: str):
    blob(uri).download_to_filename(path)


def listing(prefix_uri: str) -> list[str]:
    b, prefix = split(prefix_uri)
    return [f"gs://{b}/{x.name}" for x in client().list_blobs(b, prefix=prefix)]


def owner() -> str:
    return f"{socket.gethostname()}/{os.getpid()}"


def claim(claim_uri: str, done_uri: str, stale: float) -> bool:
    """True if this process now holds the unit (and it is not done)."""
    if exists(done_uri):
        return False
    body = json.dumps({"owner": owner(), "at": datetime.now(timezone.utc).isoformat()}).encode()
    bl = blob(claim_uri)
    try:
        bl.upload_from_string(body, if_generation_match=0)
        return True
    except gexc.PreconditionFailed:
        pass
    try:
        bl.reload()
    except gexc.NotFound:
        return False
    age = time.time() - bl.time_created.timestamp()
    if age < stale or exists(done_uri):
        return False
    try:
        bl.delete(if_generation_match=bl.generation)
        bl2 = blob(claim_uri)
        bl2.upload_from_string(body, if_generation_match=0)
        return True
    except (gexc.PreconditionFailed, gexc.NotFound):
        return False


# ------------------------------------------------------------------------------------------------------------ gsutil
_NO_MATCH = "matched no objects"


def _gsutil(*args: str, data: bytes | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(["gsutil", *args], input=data, capture_output=True, timeout=900)


def _failed(what: str, p: subprocess.CompletedProcess) -> RuntimeError:
    return RuntimeError(f"gsutil {what}: exit {p.returncode}: {p.stderr.decode('utf-8', 'replace').strip()[-500:]}")


def _gs_stat(uri: str) -> tuple[float, int] | None:
    """(time created as a Unix time, generation) of an object, None when it does not exist."""
    p = _gsutil("ls", "-la", uri)
    if p.returncode != 0:
        if _NO_MATCH in p.stderr.decode("utf-8", "replace"):
            return None
        raise _failed(f"ls -la {uri}", p)
    for line in p.stdout.decode().splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[2].startswith(uri + "#"):
            created = datetime.strptime(parts[1], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
            return created, int(parts[2].rsplit("#", 1)[1])
    raise RuntimeError(f"gsutil ls -la {uri}: no line for the object in {p.stdout[:300]!r}")


def gs_exists(uri: str) -> bool:
    return _gs_stat(uri) is not None


def gs_read_bytes(uri: str) -> bytes:
    p = _gsutil("-q", "cp", uri, "-")
    if p.returncode != 0:
        raise _failed(f"cp {uri} -", p)
    return p.stdout


def gs_write_bytes(uri: str, data: bytes, content_type: str = "application/octet-stream", only_new: bool = False):
    pre = ["-h", "x-goog-if-generation-match:0"] if only_new else []
    p = _gsutil("-q", "-h", f"Content-Type:{content_type}", *pre, "cp", "-", uri, data=data)
    if p.returncode != 0:
        if only_new and " 412 " in p.stderr.decode("utf-8", "replace"):
            raise gexc.PreconditionFailed(f"{uri} exists")
        raise _failed(f"cp - {uri}", p)


def gs_listing(prefix_uri: str) -> list[str]:
    split(prefix_uri)
    p = _gsutil("ls", prefix_uri + "**")
    if p.returncode != 0:
        if _NO_MATCH in p.stderr.decode("utf-8", "replace"):
            return []
        raise _failed(f"ls {prefix_uri}**", p)
    return [x for x in p.stdout.decode().splitlines() if x.startswith("gs://")]


def gs_claim(claim_uri: str, done_uri: str, stale: float) -> bool:
    """claim() through gsutil: the same preconditions (create only if absent, delete only this generation)."""
    if gs_exists(done_uri):
        return False
    body = json.dumps({"owner": owner(), "at": datetime.now(timezone.utc).isoformat()}).encode()
    try:
        gs_write_bytes(claim_uri, body, "application/json", only_new=True)
        return True
    except gexc.PreconditionFailed:
        pass
    st = _gs_stat(claim_uri)
    if st is None or time.time() - st[0] < stale or gs_exists(done_uri):
        return False
    p = _gsutil("-q", "-h", f"x-goog-if-generation-match:{st[1]}", "rm", claim_uri)
    if p.returncode != 0:
        err = p.stderr.decode("utf-8", "replace")
        if " 412 " in err or _NO_MATCH in err or "No URLs matched" in err:
            return False  # another process took it over first
        raise _failed(f"rm {claim_uri}", p)
    try:
        gs_write_bytes(claim_uri, body, "application/json", only_new=True)
        return True
    except gexc.PreconditionFailed:
        return False


def use_gsutil():
    """Every bucket call of this process through gsutil (see the module docstring)."""
    g = globals()
    g.update(exists=gs_exists, read_bytes=gs_read_bytes, write_bytes=gs_write_bytes, listing=gs_listing, claim=gs_claim)
