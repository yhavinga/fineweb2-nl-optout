import threading

import gcs


def test_client_per_process_and_thread(monkeypatch):
    made = []
    monkeypatch.setattr(gcs.storage, "Client", lambda: made.append(object()) or made[-1])
    monkeypatch.setattr(gcs, "_local", threading.local())
    a = gcs.client()
    assert gcs.client() is a
    other = []
    t = threading.Thread(target=lambda: other.append(gcs.client()))
    t.start()
    t.join()
    assert other[0] is not a
    monkeypatch.setattr(gcs.os, "getpid", lambda: -1)  # as in a forked child
    assert gcs.client() is not a
    assert len(made) == 3


FAKE_GSUTIL = r'''#!/usr/bin/env python3
"""gsutil for the test: ls [-la], cp (- for stdin/stdout), rm, with x-goog-if-generation-match, on a directory."""
import json, os, sys, time
store = os.environ["FAKE_GCS"]
args, hdr = sys.argv[1:], {}
while args and args[0] in ("-q", "-h"):
    if args[0] == "-h":
        k, _, v = args[1].partition(":")
        hdr[k.lower()] = v
        args = args[2:]
    else:
        args = args[1:]
def path(uri):
    return os.path.join(store, uri.replace("/", "%"))
def meta(uri):
    with open(path(uri) + ".meta") as f:
        return json.load(f)
def objects():
    return sorted(n.replace("%", "/") for n in os.listdir(store) if not n.endswith(".meta"))
def fail(msg):
    sys.stderr.write(msg + "\n")
    sys.exit(1)
pre = hdr.get("x-goog-if-generation-match")
cmd = args[0]
if cmd == "ls":
    la = args[1] == "-la"
    uri = args[-1]
    deep = uri.endswith("**")
    hits = [o for o in objects() if (o.startswith(uri[:-2]) if deep else o == uri)]
    if not hits:
        fail("CommandException: One or more URLs matched no objects.")
    for o in hits:
        m = meta(o)
        print(f"{m['size']:>10}  {m['created']}  {o}#{m['gen']}  metageneration=1" if la else o)
elif cmd == "cp":
    src, dst = args[1], args[2]
    if src == "-":
        if pre == "0" and os.path.exists(path(dst)):
            fail("ResumableUploadAbortException: 412 At least one of the pre-conditions you specified did not hold.")
        data = sys.stdin.buffer.read()
        with open(path(dst), "wb") as f:
            f.write(data)
        created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - float(os.environ.get("FAKE_AGE", "0"))))
        with open(path(dst) + ".meta", "w") as f:
            json.dump({"size": len(data), "created": created, "gen": time.time_ns()}, f)
    else:
        if not os.path.exists(path(src)):
            fail(f"CommandException: No URLs matched: {src}")
        with open(path(src), "rb") as f:
            sys.stdout.buffer.write(f.read())
elif cmd == "rm":
    uri = args[1]
    if not os.path.exists(path(uri)):
        fail(f"CommandException: No URLs matched: {uri}")
    if pre is not None and str(meta(uri)["gen"]) != pre:
        fail("PreconditionException: 412 At least one of the pre-conditions you specified did not hold.")
    os.remove(path(uri))
    os.remove(path(uri) + ".meta")
else:
    fail("unknown command " + cmd)
'''


def test_gsutil_backend(monkeypatch, tmp_path):
    import pytest
    from google.api_core import exceptions as gexc

    bindir, store = tmp_path / "bin", tmp_path / "store"
    bindir.mkdir()
    store.mkdir()
    (bindir / "gsutil").write_text(FAKE_GSUTIL)
    (bindir / "gsutil").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bindir}:{gcs.os.environ['PATH']}")
    monkeypatch.setenv("FAKE_GCS", str(store))
    for name in ("exists", "read_bytes", "write_bytes", "listing", "claim"):
        monkeypatch.setattr(gcs, name, getattr(gcs, name))  # restored after the test
    gcs.use_gsutil()
    assert gcs.exists is gcs.gs_exists and gcs.claim is gcs.gs_claim

    r = "gs://b/r"
    assert not gcs.exists(f"{r}/a.json") and gcs.listing(f"{r}/work/") == []
    gcs.write_bytes(f"{r}/a.json", b"one", "application/json", only_new=True)
    assert gcs.exists(f"{r}/a.json") and gcs.read_bytes(f"{r}/a.json") == b"one"
    with pytest.raises(gexc.PreconditionFailed):
        gcs.write_bytes(f"{r}/a.json", b"two", only_new=True)
    gcs.write_bytes(f"{r}/a.json", b"two")
    assert gcs.read_bytes(f"{r}/a.json") == b"two"
    with pytest.raises(RuntimeError, match="No URLs matched"):
        gcs.read_bytes(f"{r}/missing.json")

    claim, done = f"{r}/work/claims/fetch3/u0", f"{r}/work/fetch3/u0.parquet"
    assert gcs.claim(claim, done, stale=3600)  # new
    assert not gcs.claim(claim, done, stale=3600)  # held, not stale
    monkeypatch.setenv("FAKE_AGE", "7200")
    gcs.write_bytes(f"{r}/work/claims/fetch3/u1", b"{}")  # a claim of two hours ago
    monkeypatch.delenv("FAKE_AGE")
    assert gcs.claim(f"{r}/work/claims/fetch3/u1", f"{r}/work/fetch3/u1.parquet", stale=3600)  # taken over
    assert not gcs.claim(f"{r}/work/claims/fetch3/u1", f"{r}/work/fetch3/u1.parquet", stale=3600)
    gcs.write_bytes(done, b"x")
    assert not gcs.claim(f"{r}/work/claims/fetch3/u9", done, stale=0)  # done: never claimed
    assert gcs.listing(f"{r}/work/claims/") == [f"{r}/work/claims/fetch3/u0", f"{r}/work/claims/fetch3/u1"]
