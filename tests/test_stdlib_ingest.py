import ast
import pathlib
import unittest

from core.ingest_pipeline.url_list_ingest import summarize

ROOT = pathlib.Path(__file__).resolve().parents[1]
STDLIB_ONLY = [
  "core/http_fetch.py",
  "core/ingest_pipeline/url_list_ingest.py",
  "core/ingest_pipeline/base_ingest.py",
  "core/archival/archiver.py",
  "core/archival/monitor_links.py",
  "core/coocur/scan.py",
]
FORBIDDEN = {"requests", "bs4", "lxml", "dateutil", "tqdm", "simplejson", "fastapi", "uvicorn"}


class StdlibIngestTest(unittest.TestCase):
  def test_summarize_title_and_description(self):
    html = '<html><head><title> A &amp; B </title><meta name="description" content=" desc "></head></html>'
    self.assertEqual(summarize(html), ("A & B", "desc"))

  def test_summarize_og_fallback_and_untitled(self):
    html = '<head><meta property="og:description" content="og text"></head>'
    self.assertEqual(summarize(html), ("Untitled", "og text"))

  def test_workflow_steps_import_no_framework_packages(self):
    for rel in STDLIB_ONLY:
      tree = ast.parse((ROOT / rel).read_text(encoding="utf-8"))
      for node in ast.walk(tree):
        names = []
        if isinstance(node, ast.Import):
          names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
          names = [node.module]
        for name in names:
          self.assertNotIn(name.split(".")[0], FORBIDDEN, f"{rel} imports {name}")


if __name__ == "__main__":
  unittest.main()
