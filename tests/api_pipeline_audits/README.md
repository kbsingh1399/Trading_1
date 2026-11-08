# API pipeline hostile-audit regressions

These tests are intentionally executable against both the baseline and the patched tree.

```bash
python -m pip install pandas pyarrow websockets fastapi pytest pytest-asyncio httpx uvicorn
npm install jsdom
python -m pytest tests/api_pipeline_audits -q
```

To prove a regression against the audited baseline, export the baseline files into a
separate directory and run:

```bash
AUDIT_REPO_ROOT=/path/to/baseline python -m pytest tests/api_pipeline_audits -q
```

`NODE_PATH` may point at an existing jsdom installation. No test contacts Chrome,
CoinGlass, ipify, or warp-cli; browser contracts use jsdom and network/CLI boundaries
are mocked. Windows byte-range locking and the real WARP client still require a
Windows production-host rehearsal.
