"""Reads row groups of a parquet file on the Hugging Face Hub with HTTP range requests, without the whole file.

The resolve URL is asked once (it redirects to a signed CDN URL that is valid for about an hour) and the byte ranges
are fetched from the CDN in parallel; pyarrow then reads the row groups from those ranges as if from the file. A
signed URL that has expired is resolved again.
"""

from __future__ import annotations

import bisect
import io
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

CHUNK = 16 * 1024 * 1024


def hf_token() -> str | None:
    p = Path.home() / ".cache" / "huggingface" / "token"
    return p.read_text().strip() if p.exists() else None


class HFParquet:
    def __init__(self, repo: str, revision: str, path: str, threads: int = 8):
        self.resolve_url = f"https://huggingface.co/datasets/{repo}/resolve/{revision}/{path}"
        self.threads = threads
        self._local = threading.local()
        self._lock = threading.Lock()
        tok = hf_token()
        self._auth = {"Authorization": f"Bearer {tok}"} if tok else {}
        self._resolve()
        tail = self._get(self.size - 8, self.size)
        if tail[4:] != b"PAR1":
            raise ValueError(f"{path}: not a parquet file")
        (mlen,) = struct.unpack("<I", tail[:4])
        # pyarrow reads the last 64 KiB first to find the footer: at least the last 1 MiB is fetched
        start = max(0, min(self.size - 8 - mlen, self.size - (1 << 20)))
        self._segments: list[tuple[int, bytes]] = [(start, self._get(start, self.size))]
        self.metadata = pq.ParquetFile(self._file()).metadata

    def _session(self) -> requests.Session:
        s = getattr(self._local, "s", None)
        if s is None:
            s = self._local.s = requests.Session()
        return s

    def _resolve(self):
        for attempt in range(6):
            try:
                r = requests.get(self.resolve_url, headers=self._auth, allow_redirects=False, timeout=60)
                if r.status_code in (301, 302, 307, 308) and "location" in r.headers:
                    with self._lock:
                        self.url = r.headers["location"]
                        self.size = int(r.headers["x-linked-size"])
                        self.etag = r.headers.get("x-linked-etag", "").strip('"')
                        self.commit = r.headers.get("x-repo-commit")
                    return
                err = f"HTTP {r.status_code}"
            except requests.RequestException as e:
                err = repr(e)
            time.sleep(min(60, 5 * 2**attempt))
        raise RuntimeError(f"resolving {self.resolve_url}: {err}")

    def _get(self, start: int, end: int) -> bytes:
        """Bytes [start, end) of the file."""
        err = None
        for attempt in range(8):
            try:
                r = self._session().get(self.url, headers={"Range": f"bytes={start}-{end - 1}"}, timeout=120)
                if r.status_code == 206 and len(r.content) == end - start:
                    return r.content
                if r.status_code in (401, 403):  # the signed URL expired
                    self._resolve()
                err = f"HTTP {r.status_code}, {len(r.content)} bytes for {end - start}"
            except requests.RequestException as e:
                err = repr(e)
            time.sleep(min(60, 2 * 2**attempt))
        raise RuntimeError(f"range {start}-{end} of {self.resolve_url}: {err}")

    def prefetch(self, ranges: list[tuple[int, int]]):
        """Fetches these byte ranges (merged when they touch, split in CHUNK pieces) in parallel."""
        merged: list[list[int]] = []
        for a, b in sorted(ranges):
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], b)
            else:
                merged.append([a, b])
        pieces = [(a, min(a + CHUNK, b)) for a, b in merged for a in range(a, b, CHUNK)]
        with ThreadPoolExecutor(self.threads) as ex:
            data = list(ex.map(lambda ab: self._get(*ab), pieces))
        self._segments = self._segments[:1] + [
            (a, d) for (a, _), d in zip(pieces, data, strict=False)
        ]  # the footer stays

    def _file(self) -> io.RawIOBase:
        return _SparseFile(self.size, self._segments)

    def column_ranges(self, row_groups: list[int], columns: list[str]) -> list[tuple[int, int]]:
        out = []
        for i in row_groups:
            rg = self.metadata.row_group(i)
            for j in range(rg.num_columns):
                c = rg.column(j)
                if c.path_in_schema in columns:
                    start = c.dictionary_page_offset if c.has_dictionary_page else c.data_page_offset
                    if start is None or start <= 0:
                        start = c.data_page_offset
                    out.append((start, start + c.total_compressed_size))
        return out

    def read(self, row_groups: list[int], columns: list[str] | None = None) -> pa.Table:
        """These row groups (and columns), fetched first."""
        cols = columns or self.metadata.schema.to_arrow_schema().names
        self.prefetch(self.column_ranges(row_groups, cols))
        return pq.ParquetFile(self._file()).read_row_groups(row_groups, columns=columns)


class _SparseFile(io.RawIOBase):
    """A read-only file of `size` bytes of which only some segments are known; reading elsewhere is an error."""

    def __init__(self, size: int, segments: list[tuple[int, bytes]]):
        self.size = size
        # segments may overlap (the footer window covers the last row groups of a small file): merged first
        merged: list[tuple[int, bytes]] = []
        for start, data in sorted(segments, key=lambda s: s[0]):
            if merged and start <= merged[-1][0] + len(merged[-1][1]):
                s0, d0 = merged[-1]
                end = start + len(data)
                if end > s0 + len(d0):
                    d0 = d0 + data[s0 + len(d0) - start :]
                merged[-1] = (s0, d0)
            else:
                merged.append((start, data))
        self.segments = merged
        self.starts = [s for s, _ in self.segments]
        self.pos = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, offset, whence=0):
        self.pos = offset if whence == 0 else (self.pos + offset if whence == 1 else self.size + offset)
        return self.pos

    def tell(self):
        return self.pos

    def readinto(self, b) -> int:
        n = min(len(b), self.size - self.pos)
        got = 0
        while got < n:
            i = bisect.bisect_right(self.starts, self.pos) - 1
            start, data = self.segments[i] if i >= 0 else (0, b"")
            if not start <= self.pos < start + len(data):
                raise OSError(f"read at {self.pos} outside the fetched ranges")
            k = min(n - got, start + len(data) - self.pos)
            b[got : got + k] = data[self.pos - start : self.pos - start + k]
            self.pos += k
            got += k
        return max(got, 0)
