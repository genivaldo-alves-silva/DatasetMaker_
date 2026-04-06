# Prose Country Sandbox

Drop your test PDFs into `./pdf` and run the sandbox launcher.

## Structure
- `pdf/`: input PDFs for testing
- `out/`: run outputs (`sections_detected.csv`, `regex_pairs.csv`, `llm_pairs.csv`, `combined_pairs.csv`, `merged_confidence_pairs.csv`, `summary_confidence.csv`)
- `run_experiment.sh`: one-command runner for batch tests

## Notes
- Uses `~/jupyter/bin/activate` automatically if present.
- Uses `OPENROUTER_API_KEY` from environment / `.env` (project style).
- Default model: `liquid/lfm-2.5-1.2b-instruct:free`.

## Optional tuning via env vars
- `LLM_MODEL`
- `LLM_TIMEOUT`
- `LLM_RETRIES`
- `LLM_RETRY_BACKOFF_S`
- `LLM_MAX_TOKENS`
- `LLM_CANDIDATE_BATCH_SIZE`
