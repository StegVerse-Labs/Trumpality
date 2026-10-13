"""Standard-library HTTP access shared by the native ingest and archival steps.

Replaces the third-party `requests` dependency. A non-2xx/3xx response raises
HTTPFetchError carrying the status code, mirroring `raise_for_status()`.
"""
from __future__ import annotations

import urllib.error
import urllib.request
from dataclasses import dataclass, field


class HTTPFetchError(Exception):
  def __init__(self, status_code: int | None, message: str):
    super().__init__(message)
    self.status_code = status_code


@dataclass
class FetchResult:
  status_code: int
  content: bytes
  headers: dict = field(default_factory=dict)

  @property
  def text(self) -> str:
    charset = "utf-8"
    content_type = self.headers.get("Content-Type", "")
    if "charset=" in content_type:
      charset = content_type.split("charset=", 1)[1].split(";", 1)[0].strip() or charset
    return self.content.decode(charset, errors="replace")


def fetch(url: str, *, user_agent: str, timeout: float = 30, method: str = "GET", read_body: bool = True) -> FetchResult:
  request = urllib.request.Request(url, method=method, headers={"User-Agent": user_agent})
  try:
    with urllib.request.urlopen(request, timeout=timeout) as response:
      body = response.read() if read_body and method != "HEAD" else b""
      return FetchResult(response.status, body, dict(response.headers.items()))
  except urllib.error.HTTPError as exc:
    raise HTTPFetchError(exc.code, f"{exc.code} {exc.reason} for url: {url}") from exc
