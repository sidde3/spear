"""triggers a real run of documents-rag-optimization-pipeline, AutoRAG's
own kfp pipeline, which rhoai auto registers on every dspa pipeline
server (confirmed via official docs, not reverse engineered), rather
than reimplementing autorag's dashboard flow ourselves.

mirrors phase1_ingestion/compile_and_run.py's auth pattern (the caller's
own `oc whoami -t` token, no separate pipeline server credential) but
does not compile or upload anything: this pipeline already exists on
rag-phase1-dspa, this script only looks it up by name and runs it.

parameter names below come from actually reading this pipeline's own
live registered input definitions on this cluster (see
inspect_params.py), not from the compiled ir pulled from github earlier
in this project's investigation: that github copy (branch
rhoai-3.4-fixed) turned out to describe an older parameter shape
(llama_stack_secret_name, llama_stack_vector_io_provider_id,
embeddings_models) that this cluster's actually registered version has
since simplified to ogx_secret_name, vector_io_provider_id and
embedding_models. always trust the live registration over any external
copy.

parameters point at:
  - sanitized/ in the raw_bucket as the corpus autorag is allowed to
    index (the sanitized only boundary sanitize_documents itself
    enforces, see phase1_ingestion/pipeline.py)
  - the eval dataset phase1_ingestion_pipeline's generate_eval_dataset
    step wrote to autorag-eval/<batch_id>_test_data.json
  - the pgvector vector_io provider already live and healthy on
    rag-phase1-ogx (confirmed via a real /v1/providers call)
  - both foundation chat models plus the embedding model, all using the
    exact ids ogx's own /v1/models exposes (confirmed live), not the
    litellm double prefixed form sdg_hub needed: autorag talks to ogx
    directly by its own client, not through litellm's openai/ custom
    api_base routing, so no double prefix is needed here.

usage: source .venv-kfp/bin/activate && python run_autorag.py [batch_id]
"""
import subprocess
import sys
import time

from kfp.client import Client

AUTORAG_PIPELINE_NAME = "documents-rag-optimization-pipeline"
EXPERIMENT_NAME = "phase1-autorag"

RAW_BUCKET = "rag-documents"
SANITIZED_PREFIX = "sanitized/"
EVAL_PREFIX = "autorag-eval/"
S3_SECRET_NAME = "autorag-s3-creds"
OGX_SECRET_NAME = "autorag-ogx-creds"
VECTOR_IO_PROVIDER_ID = "pgvector"

# ids exactly as returned by ogx's own /v1/models, confirmed live.
# the embedding model changed on 2026-09-27: nomic-embed-text-v2-moe via
# litemaas is hard capped at 512 tokens server side (below the 700 token
# minimum this pipeline itself requires), replaced with a self hosted
# tei server serving nomic-embed-text-v1.5 at a real 8192 token context,
# see manifests/spear-inference/01-ogxserver.yaml and
# 02-embed-server.yaml
#
# glm-53-flash was genuinely unusable on 2026-09-28: ai4rag's own search
# space preparation step checks ogx's /v1/models listing (never makes a
# real chat completion call itself), and glm-53-flash had not appeared
# in that listing yet, so the run failed with
# `SearchSpaceValueError: Provided models of type 'llm' are not
# registered in OGX: ['openai/publishers/prelude-maas/models/
# glm-53-flash']`, see PHASE1_PLAN.md. routable is not the same thing as
# registered, autorag only ever cares about the latter. this was fixed
# on the ogx side (model listing now includes it, confirmed live in a
# later session after an intervening pod restart), so glm-53-flash is
# back in this list.
#
# note: even with both models correctly accepted into the search space
# (confirmed live in search_space_prep_report's foundation_model axis),
# a given run's actual 8 patterns can still end up all using one model.
# that is ai4rag's own GAMOptimizer, a seeded surrogate model search
# capped at max_evals=8 over a combinatorial space of hundreds of
# chunking/retrieval/model combinations, not a config bug here. confirmed
# live for one real run where all 8 real objective function evaluations
# logged foundation_model=glm-53-flash, qwen3.6-35b-a3b never sampled.
GENERATION_MODELS = [
    "vllm-inference/gpt-oss-120b",
    "openai/publishers/prelude-maas/models/glm-53-flash",
]
# only 1 embedding model, not the section 5 spec's 2: confirmed by
# reading ogx's own baked in config.yaml directly that
# registered_resources.models has exactly one static embedding entry
# templated off scalar EMBEDDING_MODEL/EMBEDDING_PROVIDER_MODEL_ID/
# EMBEDDING_DIMENSION env vars, not a list, unlike the two independent
# generation slots (remote::openai plus remote::vllm). this vendored
# ogx build structurally cannot expose a second embedding model at the
# same time, a proxy in front of it (the same trick used for the judge
# fix) cannot help since /v1/models would still only ever list one id.
# decision, 2026-09-27: ship phase 1 with this disclosed platform
# limitation rather than burn time on a second self hosted embedding
# server for a comparison ogx cannot actually run in one pass, see
# PHASE1_PLAN.md
EMBEDDING_MODELS = ["vllm-embedding/nomic-embed-text-v1.5"]

# production recipe per PHASE1_PLAN.md section 5, raised from the
# 2026-09-26 first validation config (4 patterns, default "speed"
# preset) now that the whole chain, including the answer_relevance
# judge fix, has been watched succeed end to end
MAX_RAG_PATTERNS = 8
PRESET = "balanced"
OPTIMIZATION_METRIC = "faithfulness"

# how long to keep polling before handing control back to the caller,
# this pipeline is a genuine multi pattern optimization sweep, not
# something meant to finish inside one interactive shell call
POLL_SECONDS = 90
POLL_INTERVAL = 10


def current_token() -> str:
    return subprocess.run(
        ["oc", "whoami", "-t"], check=True, capture_output=True, text=True
    ).stdout.strip()


def dspa_route() -> str:
    # resolved live, this route hostname is unique per cluster, never hardcode it
    host = subprocess.run(
        ["oc", "get", "route", "ds-pipeline-rag-phase1-dspa", "-n", "spear-pipelines",
         "-o", "jsonpath={.spec.host}"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    return f"https://{host}"


def main() -> None:
    batch_id = sys.argv[1] if len(sys.argv) > 1 else "seed-batch-001"
    test_data_key = f"{EVAL_PREFIX}{batch_id}_test_data.json"

    route = dspa_route()
    client = Client(host=route, existing_token=current_token())

    existing = client.list_pipelines(
        filter=(
            '{"predicates":[{"key":"name","operation":"EQUALS",'
            f'"stringValue":"{AUTORAG_PIPELINE_NAME}"}}]}}'
        )
    )
    pipelines = existing.pipelines or []
    if not pipelines:
        raise RuntimeError(
            f"{AUTORAG_PIPELINE_NAME} not found on {route}. rhoai "
            "docs say this is auto registered whenever the pipeline "
            "server starts, so either this dspa predates that "
            "registration or the pipeline server needs a restart."
        )
    pipeline_id = pipelines[0].pipeline_id
    versions = client.list_pipeline_versions(pipeline_id).pipeline_versions or []
    if not versions:
        raise RuntimeError(f"{AUTORAG_PIPELINE_NAME} has no versions registered")
    version_id = versions[0].pipeline_version_id
    print(f"found {AUTORAG_PIPELINE_NAME}: pipeline_id={pipeline_id} version_id={version_id}")

    experiment = client.create_experiment(name=EXPERIMENT_NAME)

    params = {
        "embedding_models": EMBEDDING_MODELS,
        "generation_models": GENERATION_MODELS,
        "input_data_bucket_name": RAW_BUCKET,
        "input_data_key": SANITIZED_PREFIX,
        "input_data_secret_name": S3_SECRET_NAME,
        "ogx_secret_name": OGX_SECRET_NAME,
        "vector_io_provider_id": VECTOR_IO_PROVIDER_ID,
        "optimization_max_rag_patterns": MAX_RAG_PATTERNS,
        "optimization_metric": OPTIMIZATION_METRIC,
        "preset": PRESET,
        "test_data_bucket_name": RAW_BUCKET,
        "test_data_key": test_data_key,
        "test_data_secret_name": S3_SECRET_NAME,
    }
    print("run parameters:")
    for k, v in params.items():
        print(f"  {k} = {v}")

    run = client.run_pipeline(
        experiment_id=experiment.experiment_id,
        job_name=f"{AUTORAG_PIPELINE_NAME}-{batch_id}",
        pipeline_id=pipeline_id,
        version_id=version_id,
        params=params,
    )
    print(f"started run {run.run_id}")

    # poll briefly, just enough to confirm it is genuinely progressing
    # rather than failing immediately, not a full wait for completion:
    # a real optimization sweep across multiple rag patterns can run for
    # a long time.
    deadline = time.time() + POLL_SECONDS
    last_state = None
    while time.time() < deadline:
        current = client.get_run(run.run_id)
        state = current.state
        if state != last_state:
            print(f"  state: {state}")
            last_state = state
        if state in ("FAILED", "ERROR"):
            raise RuntimeError(f"run {run.run_id} entered state {state} during the initial poll window")
        if state in ("SUCCEEDED", "SKIPPED"):
            break
        time.sleep(POLL_INTERVAL)

    print(f"\nrun {run.run_id} is genuinely progressing (last observed state: {last_state}).")
    print(f"watch it in the dashboard under Experiments > {EXPERIMENT_NAME}, or resume polling with:")
    print(
        f"  python -c \"from kfp.client import Client; "
        f"c = Client(host='{route}', existing_token='<token>'); "
        f"print(c.get_run('{run.run_id}').state)\""
    )
    print("leaderboard/winning pattern will land under this run's own artifacts once it finishes.")


if __name__ == "__main__":
    main()
