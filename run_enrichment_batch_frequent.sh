#!/bin/bash
# Frequent enrichment with configurable batch size
cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia && \
python 03_Scripts_Code/scripts/run_enrichment_batch.py --all --resume "$@"