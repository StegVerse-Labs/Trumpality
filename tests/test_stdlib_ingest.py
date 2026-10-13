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
  "api/app.py",
  "scripts/search_agent.py",
  "scripts/erl_research_agent.py",
]
FORBIDDEN = {"requests", "bs4", "lxml", "dateutil", "tqdm", "simplejson", "fastapi", "uvicorn", "pandas", "feedparser"}


class StdlibIngestTest(unittest.TestCase):
  def test_summarize_title_and_description(self):
    html = '<html><head><title> A &amp; B </title><meta name="description" content=" desc "></head></html>'
    self.assertEqual(summarize(html), ("A & B", "desc"))

  def test_summarize_og_fallback_and_untitled(self):
    html = '<head><meta property="og:description" content="og text"></head>'
    self.assertEqual(summarize(html), ("Untitled", "og text"))

  def test_search_agent_parses_rss_and_atom(self):
    import importlib.util
    spec = importlib.util.spec_from_file_location("search_agent", ROOT / "scripts" / "search_agent.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    rss = '<rss><channel><item><title>T1</title><link>https://a.example/1</link><description>D1</description><category>c</category></item></channel></rss>'
    atom = '<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>T2</title><link href="https://a.example/2"/><summary>D2</summary></entry></feed>'
    self.assertEqual(module.parse_feed(rss)[0]["link"], "https://a.example/1")
    self.assertEqual(module.parse_feed(rss)[0]["tags"], "c")
    self.assertEqual(module.parse_feed(atom)[0]["link"], "https://a.example/2")
    self.assertEqual(module.parse_feed(atom)[0]["summary"], "D2")

  def test_no_framework_requirements_file(self):
    self.assertFalse((ROOT / "core" / "requirements.txt").exists())

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
