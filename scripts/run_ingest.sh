#!/usr/bin/env bash
set -e
python core/ingest_pipeline/url_list_ingest.py \
  --subject "Donald J. Trump" \
  --topic_cluster general \
  --urls seeds/sources.urls.txt
