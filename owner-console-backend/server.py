# backend for the phase1-ingestion-console ConsolePlugin's spec.proxy, see
# PHASE1_PLAN.md section 6 plus the "trigger and business value" addendum.
#
# unlike the first version of this file, this one is no longer stdlib only:
# reading a completed autorag run's per pattern scores needs a real signed s3
# get (boto3), and triggering kfp runs correctly needs the real kfp rest field
# names rather than a hand rolled json body, so this now ships as a built
# image (see owner-console-backend/Dockerfile) instead of a configmap mounted
# script.
#
# mlflow reads still use this pod's own service account token, unchanged from
# before. anything that touches the kfp pipeline server (listing kfp runs,
# reading autorag pattern scores, triggering a run) uses the signed in user's
# own forwarded token instead, since the consoleplugin proxy alias is now
# authorization: UserToken, not None. that means a user can only trigger or
# inspect kfp runs if their own openshift identity already has rbac on
# spear-pipelines' own pipeline resources, this backend's own service
# account never gains that power just by existing.
import json
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
from kubernetes import client as k8s_client, config as k8s_config
from kubernetes.stream import stream as k8s_stream

# mlflow's external url, the dspa pipeline server's route, minio's own
# console route and the rhoai dashboard route are all unique per cluster,
# never hardcode any of them: all four are resolved once at deploy time and
# injected here as env vars, see manifests/spear-console/01-backend.yaml
# and scripts/08-install-console.sh
MLFLOW_URL = os.environ["MLFLOW_URL"]
DSPA_ROUTE = os.environ["DSPA_ROUTE"]
GUARDRAILS_ROUTE = os.environ["GUARDRAILS_ROUTE"]
MINIO_CONSOLE_URL = os.environ["MINIO_CONSOLE_URL"]
RHOAI_DASHBOARD_URL = os.environ["RHOAI_DASHBOARD_URL"]
TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"


def _env_list(name, default_csv):
    # every pipeline/experiment name, bucket, secret name and autorag knob
    # below used to be a python constant baked into the image, so changing
    # any of them meant a rebuild. moved to env vars 2026-09-28 so an
    # operator can retune this deployment with `oc set env` alone, same
    # spirit as the route vars above. defaults below match this project's
    # own known good values, so an unpatched deployment behaves exactly as
    # before, see scripts/08-install-console.sh for where these are
    # actually set explicitly rather than left to fall back on a default.
    raw = os.environ.get(name, default_csv)
    return [item.strip() for item in raw.split(",") if item.strip()]


# NAMESPACE_MIGRATION_PLAN.md: the mlflow workspace label moved from
# rag-phase1 to spear-pipelines, see manifests/spear-pipelines/03-mlflow.yaml
WORKSPACE = os.environ.get("RAG_WORKSPACE", "spear-pipelines")
INGESTION_EXPERIMENT_NAME = os.environ.get("INGESTION_EXPERIMENT_NAME", "phase1-ingestion")
# the real experiment scripts/11-submit-evalhub-jobs.sh writes every real
# eval cycle's results into, see create_eval_cycle_run.py and
# log_eval_to_mlflow.py, same workspace WORKSPACE above already points at
EVAL_RESULTS_EXPERIMENT_NAME = os.environ.get(
    "EVAL_RESULTS_EXPERIMENT_NAME", "spear-shield-evalhub-results"
)

INGESTION_PIPELINE_NAME = os.environ.get("INGESTION_PIPELINE_NAME", "phase1-ingestion-pipeline")
INGESTION_KFP_EXPERIMENT_NAME = os.environ.get("INGESTION_KFP_EXPERIMENT_NAME", "phase1-ingestion")
AUTORAG_PIPELINE_NAME = os.environ.get("AUTORAG_PIPELINE_NAME", "documents-rag-optimization-pipeline")
AUTORAG_KFP_EXPERIMENT_NAME = os.environ.get("AUTORAG_KFP_EXPERIMENT_NAME", "phase1-autorag")
APPLY_PATTERN_PIPELINE_NAME = os.environ.get(
    "APPLY_PATTERN_PIPELINE_NAME", "phase1-apply-pattern-pipeline"
)
APPLY_PATTERN_KFP_EXPERIMENT_NAME = os.environ.get(
    "APPLY_PATTERN_KFP_EXPERIMENT_NAME", "phase1-apply-pattern"
)

# these mirror run_autorag.py's own defaults, confirmed live against this
# cluster's own registered pipeline, not from any external copy of the
# pipeline. kept in sync by hand since this backend intentionally does not
# import that script (it lives outside the built image's context). the
# model lists are the two most likely to need retuning without a rebuild,
# e.g. reverting to glm-53-flash once it is confirmed live again in ogx's
# own /v1/models, see PHASE1_PLAN.md.
AUTORAG_RAW_BUCKET = os.environ.get("AUTORAG_RAW_BUCKET", "rag-documents")
AUTORAG_S3_SECRET_NAME = os.environ.get("AUTORAG_S3_SECRET_NAME", "autorag-s3-creds")
AUTORAG_OGX_SECRET_NAME = os.environ.get("AUTORAG_OGX_SECRET_NAME", "autorag-ogx-creds")
AUTORAG_VECTOR_IO_PROVIDER_ID = os.environ.get("AUTORAG_VECTOR_IO_PROVIDER_ID", "pgvector")
AUTORAG_EMBEDDING_MODELS = _env_list(
    "AUTORAG_EMBEDDING_MODELS", "vllm-embedding/nomic-embed-text-v1.5"
)
AUTORAG_GENERATION_MODELS = _env_list(
    "AUTORAG_GENERATION_MODELS",
    "vllm-inference/gpt-oss-120b,openai/publishers/prelude-maas/models/glm-53-flash",
)
AUTORAG_MAX_RAG_PATTERNS = int(os.environ.get("AUTORAG_MAX_RAG_PATTERNS", "8"))
AUTORAG_PRESET = os.environ.get("AUTORAG_PRESET", "balanced")
AUTORAG_OPTIMIZATION_METRIC = os.environ.get("AUTORAG_OPTIMIZATION_METRIC", "faithfulness")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
ARTIFACT_BUCKET = os.environ.get("ARTIFACT_BUCKET", "dsp-pipeline-artifacts")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "")
# the knowledge base page's read-only source strip, the pipeline's own view
# of where sanitization reads from: the bare host:port form the kfp pipeline
# itself defaults to (pipelines/phase1_ingestion/pipeline.py's
# MINIO_ENDPOINT_DEFAULT, distinct from the http:// prefixed MINIO_ENDPOINT
# above which boto3 needs), the raw intake bucket/prefix every ingestion run
# lists, and the minio console browser link for the manifest. secret names
# and credential values are deliberately excluded, the secret is mounted
# into the task pods by the pipeline itself and never travels through this
# backend.
#
# these three are the fallback only now, see ingestion_source_config below:
# the real values come from the registered pipeline's own compiled spec so
# this never silently drifts from pipeline.py's own dsl.pipeline defaults
# the way AUTORAG_RAW_BUCKET already quietly does above. only used if that
# live lookup fails, e.g. right after a fresh install before anyone has
# run pipelines/phase1_ingestion/compile_and_run.py yet
INGESTION_SOURCE_ENDPOINT = os.environ.get(
    "INGESTION_SOURCE_ENDPOINT", "minio.minio.svc.cluster.local:9000"
)
INGESTION_SOURCE_BUCKET = os.environ.get("INGESTION_SOURCE_BUCKET", AUTORAG_RAW_BUCKET)
INGESTION_SOURCE_PREFIX = os.environ.get("INGESTION_SOURCE_PREFIX", "raw-intake/")

# phase 2's spear shield chat tab, PHASE2_PLAN.md section 8. this is the
# first endpoint on this backend that never touches mlflow or kfp at all,
# it calls out to spear-coordinator-agent instead, through the a2a gateway
# in the spear-shield-agents namespace. see manifests/spear-console/02-chat-creds.yaml
# for why the per persona password lookup below is a demo-only shortcut,
# not a real identity federation
SPEAR_SHIELD_ISSUER_URL = os.environ.get("SPEAR_SHIELD_ISSUER_URL", "")
SPEAR_COORDINATOR_A2A_URL = os.environ.get("SPEAR_COORDINATOR_A2A_URL", "")
SPEAR_COORDINATOR_CLIENT_ID = os.environ.get("SPEAR_COORDINATOR_CLIENT_ID", "spear-coordinator")
SPEAR_SHIELD_CREDS_DIR = os.environ.get("SPEAR_SHIELD_CREDS_DIR", "/secrets/spear-shield-console-creds")
SPEAR_COORDINATOR_CLIENT_SECRET_PATH = os.environ.get(
    "SPEAR_COORDINATOR_CLIENT_SECRET_PATH", "/secrets/spear-coordinator-client-secret/client-secret"
)

# architecture status page + security demo page, second pass on section 8.
# these two read and exec directly against the real sandbox pods using this
# pod's own service account, not the forwarded user token, see
# manifests/spear-shield-agents/08-console-backend-rbac.yaml: this is "what does
# the real deployment look like right now" information for an audience, not
# anything scoped to whoever happens to be signed into the console
SPEAR_SHIELD_NAMESPACE = os.environ.get("SPEAR_SHIELD_NAMESPACE", "spear-shield-agents")
COORDINATOR_POD = os.environ.get("SPEAR_SHIELD_COORDINATOR_POD", "default--spear-coordinator")
RETRIEVAL_POD = os.environ.get("SPEAR_SHIELD_RETRIEVAL_POD", "default--spear-retrieval")
SPEAR_SHIELD_MCP_GATEWAY_URL = os.environ.get(
    "SPEAR_SHIELD_MCP_GATEWAY_URL",
    "http://spear-shield-gateway-istio.spear-shield-agents.svc.cluster.local:8080/mcp",
)
SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER = os.environ.get("SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER", "")
# the exact same literal url spear-coordinator-agent's own main.py already
# calls, reused here so the whoami demo exec below hits the one real
# endpoint the openshell supervisor's token_grant provider actually
# matches on, see coordinator-a2a-token-grant.yaml
RETRIEVAL_A2A_INTERNAL_URL = os.environ.get(
    "RETRIEVAL_A2A_INTERNAL_URL",
    "http://spear-retrieval-a2a-gateway.spear-shield-agents.svc.cluster.local:80",
)
# same sidecar proxy address the sandboxed agents' own http_client.py falls
# back to, confirmed live by reading env inside a real running sandbox pod,
# neither HTTPS_PROXY nor HTTP_PROXY is actually set there
SANDBOX_PROXY_CURL = "curl -s -x http://127.0.0.1:3128"
# the retrieval sandbox's own rag-query-relay endpoint, literal host:port
# straight out of manifests/spear-shield-agents/openshell/policies/
# retrieval-policy.yaml, an in cluster service name, no wildcard route to
# resolve so no env var indirection needed here the way guardrails/mlflow
# need one
RAG_QUERY_RELAY_URL = "http://spear-shield-rag-query-relay.spear-data.svc.cluster.local:8080"


def sa_token():
    with open(TOKEN_PATH) as f:
        return f.read().strip()


def mlflow_headers():
    return {
        "X-MLflow-Workspace": WORKSPACE,
        "Authorization": f"Bearer {sa_token()}",
        "Content-Type": "application/json",
    }


def _no_verify_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def mlflow_post(path, body):
    req = urllib.request.Request(
        f"{MLFLOW_URL}{path}",
        data=json.dumps(body).encode(),
        headers=mlflow_headers(),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30, context=_no_verify_ctx()) as resp:
        return json.loads(resp.read())


def mlflow_get_artifact(run_id, path):
    url = f"{MLFLOW_URL}/get-artifact?path={path}&run_uuid={run_id}"
    req = urllib.request.Request(url, headers=mlflow_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=30, context=_no_verify_ctx()) as resp:
        return resp.read()


def mlflow_list_artifacts(run_id):
    url = f"{MLFLOW_URL}/api/2.0/mlflow/artifacts/list?run_id={run_id}"
    req = urllib.request.Request(url, headers=mlflow_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=30, context=_no_verify_ctx()) as resp:
        return json.loads(resp.read()).get("files", [])


def mlflow_get_run(run_id):
    # unlike runs/search, runs/get is a real get only endpoint, confirmed
    # live, posting a json body to it 404s.
    url = f"{MLFLOW_URL}/api/2.0/mlflow/runs/get?run_id={run_id}"
    req = urllib.request.Request(url, headers=mlflow_headers(), method="GET")
    with urllib.request.urlopen(req, timeout=30, context=_no_verify_ctx()) as resp:
        return json.loads(resp.read())


def find_ingestion_experiment():
    experiments = mlflow_post("/api/2.0/mlflow/experiments/search", {"max_results": 100})
    return next(
        (e for e in experiments.get("experiments", []) if e["name"] == INGESTION_EXPERIMENT_NAME),
        None,
    )


def recent_ingestion_runs(max_results=20):
    # lists the most recent ingestion runs so the console can offer a picker
    # instead of only ever jumping straight to the latest one.
    exp = find_ingestion_experiment()
    if exp is None:
        return []
    runs = mlflow_post(
        "/api/2.0/mlflow/runs/search",
        {
            "experiment_ids": [exp["experiment_id"]],
            "max_results": max_results,
            "order_by": ["attributes.start_time DESC"],
        },
    )
    out = []
    for run in runs.get("runs", []):
        info = run["info"]
        out.append(
            {
                "run_id": info["run_id"],
                "run_name": info.get("run_name"),
                "start_time": info.get("start_time"),
                "status": info.get("status"),
            }
        )
    return out


def ingestion_report(run_id=None):
    exp = find_ingestion_experiment()
    if exp is None:
        return {"error": f"No {INGESTION_EXPERIMENT_NAME!r} experiment found in workspace {WORKSPACE!r}"}

    if run_id:
        got = mlflow_get_run(run_id)
        run = got.get("run")
        if run is None:
            return {"error": f"Run {run_id!r} not found"}
    else:
        runs = mlflow_post(
            "/api/2.0/mlflow/runs/search",
            {
                "experiment_ids": [exp["experiment_id"]],
                "max_results": 1,
                "order_by": ["attributes.start_time DESC"],
            },
        )
        run_list = runs.get("runs", [])
        if not run_list:
            return {"error": "Experiment exists but has no runs yet"}
        run = run_list[0]

    resolved_run_id = run["info"]["run_id"]
    report_bytes = mlflow_get_artifact(resolved_run_id, "ingestion_report.json")
    report = json.loads(report_bytes)
    report["_mlflow_run_id"] = resolved_run_id
    report["_mlflow_run_name"] = run["info"].get("run_name")
    report["_mlflow_run_url"] = mlflow_run_link(run["info"]["experiment_id"], resolved_run_id)

    # every document this report mentions, quarantined or merely audited,
    # always still has its original copy under raw-intake/, confirmed live
    # against pipeline.py: quarantined docs never leave raw-intake/, and a
    # sanitize verdict of "success" only ever adds a second copy under
    # sanitized/, it never removes the raw-intake/ one. linking to
    # raw-intake/ is therefore always correct, pass, block or quarantine.
    for doc in report.get("ingestion", {}).get("quarantined_detail", []):
        doc["minio_url"] = minio_object_link(AUTORAG_RAW_BUCKET, f"raw-intake/{doc['filename']}")
    for entry in report.get("guardrails", {}).get("audit", []):
        entry["minio_url"] = minio_object_link(AUTORAG_RAW_BUCKET, f"raw-intake/{entry['filename']}")
    return report


# ---- eval cycle trend, reading the real runs 11-submit-evalhub-jobs.sh's
# own create_eval_cycle_run.py/log_eval_to_mlflow.py already write, nothing
# new logged, just read back and shaped for a chart. field names below are
# not guessed, confirmed live against this project's own real mlflow runs
# after chasing and fixing the wait_for_job timeout bug that used to drop
# ragas and ibm-clear from the two most recent cycles, see PHASE3_PLAN.md
# section 7's own "a real regression found while building a trend view"
# note for the full story. ----

def find_eval_results_experiment():
    experiments = mlflow_post("/api/2.0/mlflow/experiments/search", {"max_results": 100})
    return next(
        (e for e in experiments.get("experiments", []) if e["name"] == EVAL_RESULTS_EXPERIMENT_NAME),
        None,
    )


def recent_eval_cycles(max_results=10):
    # one entry per real cycle 11-submit-evalhub-jobs.sh ran, oldest first
    # so a trend chart reads left to right. each cycle's own benchmarks
    # list is read back the same way mlflow's own run list already groups
    # them, by the real mlflow.parentRunId tag every child run carries.
    exp = find_eval_results_experiment()
    if exp is None:
        return []

    parents = mlflow_post(
        "/api/2.0/mlflow/runs/search",
        {
            "experiment_ids": [exp["experiment_id"]],
            "filter": "tags.`evalhub.cycle` = 'true'",
            "max_results": max_results,
            "order_by": ["attributes.start_time DESC"],
        },
    ).get("runs", [])

    cycles = []
    for parent in parents:
        parent_id = parent["info"]["run_id"]
        children = mlflow_post(
            "/api/2.0/mlflow/runs/search",
            {
                "experiment_ids": [exp["experiment_id"]],
                "filter": f"tags.`mlflow.parentRunId` = '{parent_id}'",
                "max_results": 20,
            },
        ).get("runs", [])

        benchmarks = []
        for child in children:
            data = child.get("data", {})
            tags = {t["key"]: t["value"] for t in data.get("tags", [])}
            metrics = {m["key"]: float(m["value"]) for m in data.get("metrics", [])}
            params = {p["key"]: p["value"] for p in data.get("params", [])}
            run_id = child["info"]["run_id"]
            provider_id = tags.get("evalhub.provider_id", "")

            # a native run (ibm-clear, the only provider that saves straight
            # to mlflow itself, see log_eval_to_mlflow.py's own header) never
            # gets the evalhub.provider_id tag this script sets, that is the
            # real signal to go look for a real artifact worth linking,
            # rather than paying for an artifact list call on every run
            report_path = None
            if not provider_id:
                for f in mlflow_list_artifacts(run_id):
                    if not f.get("is_dir") and f.get("path", "").endswith(".html"):
                        report_path = f["path"]
                        break

            benchmarks.append(
                {
                    "run_id": run_id,
                    # job_name is the one label every benchmark type
                    # reliably carries: ibm-clear's own native run (the
                    # provider's own save path, this script only tags it,
                    # never logs its metrics) never gets the
                    # provider_id/benchmark_id pair the other three do,
                    # confirmed live, so job_name is what a chart legend
                    # should key off, not provider_id
                    "job_name": tags.get("evalhub.job_name", ""),
                    "provider_id": provider_id or params.get("provider_id", ""),
                    "benchmark_id": tags.get("evalhub.benchmark_id", "") or params.get("benchmark_id", ""),
                    "metrics": metrics,
                    # model/threshold/duration/sample size, whichever of
                    # these a given run actually logged, read straight
                    # back, nothing recomputed, see log_eval_to_mlflow.py
                    # and ibm-clear's own native params for the real shape
                    "params": params,
                    "report_url": (
                        f"/trend/artifact?run_id={run_id}&path={urllib.parse.quote(report_path)}"
                        if report_path
                        else None
                    ),
                }
            )

        cycles.append(
            {
                "run_id": parent_id,
                "run_name": parent["info"].get("run_name"),
                "start_time": parent["info"].get("start_time"),
                "mlflow_url": mlflow_run_link(exp["experiment_id"], parent_id),
                "benchmarks": benchmarks,
            }
        )

    cycles.reverse()
    return cycles


# ---- kfp pipeline server rest helpers, all authenticated with the caller's
# own forwarded token, never this pod's own identity, see the header comment
# above. field names confirmed against kfp_server_api's generated models and
# live curl calls against this cluster's own dspa route, 2026-09-28, not
# guessed from the kfp sdk's python surface. ----

def kfp_headers(user_token):
    return {"Authorization": f"Bearer {user_token}", "Content-Type": "application/json"}


def kfp_get(path, user_token):
    req = urllib.request.Request(f"{DSPA_ROUTE}{path}", headers=kfp_headers(user_token), method="GET")
    with urllib.request.urlopen(req, timeout=30, context=_no_verify_ctx()) as resp:
        return json.loads(resp.read())


def kfp_post(path, body, user_token):
    req = urllib.request.Request(
        f"{DSPA_ROUTE}{path}",
        data=json.dumps(body).encode(),
        headers=kfp_headers(user_token),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30, context=_no_verify_ctx()) as resp:
        return json.loads(resp.read())


def kfp_name_filter(display_name):
    return json.dumps(
        {"predicates": [{"key": "display_name", "operation": "EQUALS", "string_value": display_name}]}
    )


def find_pipeline_id(pipeline_name, user_token):
    q = urllib.parse.quote(kfp_name_filter(pipeline_name))
    resp = kfp_get(f"/apis/v2beta1/pipelines?filter={q}", user_token)
    pipelines = resp.get("pipelines", [])
    if not pipelines:
        raise LookupError(f"No KFP pipeline named {pipeline_name!r} is registered on this pipeline server yet")
    return pipelines[0]["pipeline_id"]


def find_latest_pipeline_version_id(pipeline_id, user_token):
    resp = kfp_get(
        f"/apis/v2beta1/pipelines/{pipeline_id}/versions?sort_by=created_at%20desc&page_size=1",
        user_token,
    )
    versions = resp.get("pipeline_versions", [])
    if not versions:
        raise LookupError(f"Pipeline {pipeline_id!r} has no versions registered")
    return versions[0]["pipeline_version_id"]


# the registered pipeline's own compiled spec is the one real source of
# truth for minio_endpoint/raw_bucket/raw_prefix, not a second hand copy of
# pipeline.py's own dsl.pipeline defaults kept here. trigger_kfp_run never
# overrides these three per run (only batch_id/guardrails_route/mlflow_url
# are passed in runtime_config.parameters), so whatever the latest
# registered version's own default says really is what every real run
# reads from today.
#
# there is no separate "get template" rest call on this kfp server, that
# was a wrong guess, confirmed live: a GET to .../templates 404s, and the
# installed kfp_server_api==2.17.0 client has no method for it at all.
# the real pipeline_service_get_pipeline_version response already embeds
# the full compiled spec under a doubly nested "pipeline_spec" key
# (the outer one is the PipelineVersion wrapper, platform_spec alongside
# it, the inner one is the actual ir), root.inputDefinitions.parameters.
# <name>.defaultValue below confirmed against that real live response,
# not against the templates endpoint this comment used to assume existed
_INGESTION_SOURCE_CACHE_TTL_SECONDS = 300
_ingestion_source_cache = {"values": None, "expires_at": 0.0}


def fetch_ingestion_pipeline_source_defaults(user_token):
    pipeline_id = find_pipeline_id(INGESTION_PIPELINE_NAME, user_token)
    version_id = find_latest_pipeline_version_id(pipeline_id, user_token)
    resp = kfp_get(f"/apis/v2beta1/pipelines/{pipeline_id}/versions/{version_id}", user_token)
    spec = resp.get("pipeline_spec", {}).get("pipeline_spec", {})
    params = spec.get("root", {}).get("inputDefinitions", {}).get("parameters", {})
    missing = [name for name in ("minio_endpoint", "raw_bucket", "raw_prefix") if name not in params]
    if missing:
        raise LookupError(f"registered pipeline spec is missing expected parameters: {missing}")
    return {
        "minio_endpoint": params["minio_endpoint"].get("defaultValue"),
        "bucket": params["raw_bucket"].get("defaultValue"),
        "prefix": params["raw_prefix"].get("defaultValue"),
    }


def ingestion_source_config(user_token):
    now = time.time()
    if _ingestion_source_cache["values"] and _ingestion_source_cache["expires_at"] > now:
        values = _ingestion_source_cache["values"]
    else:
        try:
            values = fetch_ingestion_pipeline_source_defaults(user_token)
            _ingestion_source_cache["values"] = values
            _ingestion_source_cache["expires_at"] = now + _INGESTION_SOURCE_CACHE_TTL_SECONDS
        except Exception:
            # pipeline not registered yet, dspa briefly unreachable, ir
            # shape changed, whatever it is, this strip still has to
            # render something rather than break the whole page over it.
            # not cached, so the very next request tries the live lookup
            # again instead of being stuck on stale fallback values
            values = {
                "minio_endpoint": INGESTION_SOURCE_ENDPOINT,
                "bucket": INGESTION_SOURCE_BUCKET,
                "prefix": INGESTION_SOURCE_PREFIX,
            }
    return {
        **values,
        "manifest_url": minio_object_link(values["bucket"], f"{values['prefix']}manifest.json"),
        "console_url": MINIO_CONSOLE_URL,
    }


def find_or_create_experiment_id(experiment_name, user_token):
    q = urllib.parse.quote(kfp_name_filter(experiment_name))
    resp = kfp_get(f"/apis/v2beta1/experiments?filter={q}", user_token)
    experiments = resp.get("experiments", [])
    if experiments:
        return experiments[0]["experiment_id"]
    created = kfp_post("/apis/v2beta1/experiments", {"display_name": experiment_name}, user_token)
    return created["experiment_id"]


def trigger_kfp_run(pipeline_name, experiment_name, job_name, parameters, user_token):
    pipeline_id = find_pipeline_id(pipeline_name, user_token)
    version_id = find_latest_pipeline_version_id(pipeline_id, user_token)
    experiment_id = find_or_create_experiment_id(experiment_name, user_token)
    body = {
        "display_name": job_name,
        "experiment_id": experiment_id,
        "pipeline_version_reference": {"pipeline_id": pipeline_id, "pipeline_version_id": version_id},
        "runtime_config": {"parameters": parameters},
    }
    run = kfp_post("/apis/v2beta1/runs", body, user_token)
    return {"run_id": run["run_id"], "display_name": run.get("display_name"), "state": run.get("state")}


def dashboard_run_link(run_id):
    # confirmed live against this cluster 2026-09-28: navigating the
    # console browser to https://<rhods-dashboard route>/pipelineRuns/<ns>
    # is honored as a valid post login redirect target by the sso flow,
    # and opendatahub-io/odh-dashboard's own GlobalPipelineRunsRoutes.tsx
    # (via deepwiki) confirms the single run detail path is exactly
    # /pipelineRuns/:namespace/runs/:runId, never guessed from memory.
    return f"{RHOAI_DASHBOARD_URL}/pipelineRuns/{WORKSPACE}/runs/{run_id}"


def mlflow_run_link(experiment_id, run_id):
    # this exact hash route, "/#/experiments/<id>/runs/<run_id>", is not
    # guessed: it is mlflow's own tracking client's default run link,
    # see mlflow/tracking/_tracking_service/client.py's get_run_link.
    # confirmed live 2026-09-28 that this project's own mlflow route
    # answers on it (a 302 into its oauth proxy's login flow, the same
    # shape every other route link here gets when curled unauthenticated).
    return f"{MLFLOW_URL}/#/experiments/{experiment_id}/runs/{run_id}"


def autorag_results_link(run_id):
    # gen-ai-studio's own autorag results page for a completed run, exact
    # path given directly and confirmed live 2026-09-28: same 301 into the
    # dashboard's own login flow that dashboard_run_link above gets when
    # curled unauthenticated, not a 404.
    return f"{RHOAI_DASHBOARD_URL}/gen-ai-studio/autorag/results/{WORKSPACE}/{run_id}"


def minio_object_link(bucket, key):
    # confirmed live against this cluster 2026-09-28 by logging into the
    # minio console (server RELEASE.2025-09-07's own embedded console) and
    # clicking through to a real object: the browser lands on
    # /browser/<bucket>/<key with every '/' percent encoded as %2F>, quote
    # with safe="" reproduces that exactly.
    return f"{MINIO_CONSOLE_URL}/browser/{bucket}/{urllib.parse.quote(key, safe='')}"


def _parse_kfp_timestamp(value):
    # kfp's created_at/finished_at are rfc3339 strings, mlflow's start_time
    # is already epoch milliseconds, this gets both onto the same footing
    # so the merged run list below can sort on one field.
    if not value:
        return 0
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return 0


def recent_kfp_runs(pipeline_name, max_results, user_token):
    # the runs list endpoint has no pipeline name filter, only experiment or
    # run field filters, so this filters client side on the job_name prefix
    # this project's own trigger scripts always use instead.
    resp = kfp_get(f"/apis/v2beta1/runs?sort_by=created_at%20desc&page_size=50", user_token)
    runs = resp.get("runs", [])
    matched = [r for r in runs if (r.get("display_name") or "").startswith(pipeline_name)]
    # gen-ai-studio's own results page only exists for autorag runs, not
    # sanitization or apply pattern ones, so this extra link is only added
    # when listing the autorag pipeline's own runs.
    is_autorag = pipeline_name == AUTORAG_PIPELINE_NAME
    return [
        {
            "run_id": r["run_id"],
            "display_name": r.get("display_name"),
            "state": r.get("state"),
            "created_at": r.get("created_at"),
            "finished_at": r.get("finished_at"),
            "dashboard_url": dashboard_run_link(r["run_id"]),
            **({"autorag_results_url": autorag_results_link(r["run_id"])} if is_autorag else {}),
        }
        for r in matched[:max_results]
    ]


def merged_ingestion_runs(user_token):
    # the run picker needs to show a run the moment it is triggered, but
    # mlflow only ever has a run once log_ingestion_report (the pipeline's
    # last step) writes it, see pipeline.py. kfp's own run list already has
    # every in flight run, so this merges the two, correlated by batch_id:
    # kfp's display_name is always f"{INGESTION_PIPELINE_NAME}-{batch_id}"
    # (set in the /trigger/ingestion handler below) and mlflow's run_name is
    # always the batch_id itself (pipeline.py's own mlflow.runName tag).
    prefix = f"{INGESTION_PIPELINE_NAME}-"
    by_batch = {}
    for r in recent_kfp_runs(INGESTION_PIPELINE_NAME, 30, user_token):
        display_name = r["display_name"] or ""
        batch_id = display_name[len(prefix):] if display_name.startswith(prefix) else display_name
        by_batch[batch_id] = {
            "batch_id": batch_id,
            "mlflow_run_id": None,
            "mlflow_status": None,
            "kfp_run_id": r["run_id"],
            "kfp_state": r["state"],
            "start_time": _parse_kfp_timestamp(r["created_at"]),
            "has_report": False,
            "dashboard_url": r["dashboard_url"],
        }
    for r in recent_ingestion_runs(30):
        batch_id = r["run_name"]
        entry = by_batch.setdefault(
            batch_id,
            {
                "batch_id": batch_id,
                "mlflow_run_id": None,
                "mlflow_status": None,
                "kfp_run_id": None,
                "kfp_state": None,
                "start_time": r["start_time"] or 0,
                "has_report": False,
                "dashboard_url": None,
            },
        )
        entry["mlflow_run_id"] = r["run_id"]
        entry["mlflow_status"] = r["status"]
        entry["has_report"] = True

    return sorted(by_batch.values(), key=lambda e: e["start_time"], reverse=True)


def autorag_leaderboard(run_id, user_token):
    # there is no single "leaderboard" artifact, confirmed live 2026-09-28
    # against a completed run: the real per pattern scores live as
    # rag_patterns/Pattern<N>/pattern.json files under the run's own
    # rag-templates-optimization task in minio, and one of pattern.json's
    # metrics is flagged optimization_metric: true, that is the field this
    # project actually optimizes on (matches run_autorag.py's own default).
    #
    # the task's own kfp task_id (the folder name minio actually uses) does
    # not reliably match the "rag-templates-optimization" dag task's own
    # task_id in run_details.task_details, confirmed live: the dag level
    # task_id and the minio folder's task_id are two different ids, most
    # likely because the dag task and the executor that actually writes
    # artifacts are separate task tree nodes that this kfp version does not
    # cleanly cross reference to each other. simpler and more robust to just
    # ask minio directly which task_id folder exists under this run's own
    # rag-templates-optimization prefix, rather than trust the task tree.
    run = kfp_get(f"/apis/v2beta1/runs/{run_id}", user_token)
    state = run.get("state")
    if state != "SUCCEEDED":
        return {"state": state, "patterns_tested": 0, "winner": None}

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    stage_prefix = f"{AUTORAG_PIPELINE_NAME}/{run_id}/rag-templates-optimization/"
    stage_listing = s3.list_objects_v2(Bucket=ARTIFACT_BUCKET, Prefix=stage_prefix, Delimiter="/")
    task_folders = [p["Prefix"] for p in stage_listing.get("CommonPrefixes", [])]
    if not task_folders:
        return {"state": state, "patterns_tested": 0, "winner": None}

    prefix = f"{task_folders[0]}rag_patterns/"
    paginator = s3.get_paginator("list_objects_v2")
    pattern_keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=ARTIFACT_BUCKET, Prefix=prefix)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith("/pattern.json")
    ]

    best = None
    for key in pattern_keys:
        body = json.loads(s3.get_object(Bucket=ARTIFACT_BUCKET, Key=key)["Body"].read())
        metrics = body.get("evaluation", {}).get("metrics", [])
        metric = next((m for m in metrics if m.get("optimization_metric")), None)
        if metric is None:
            continue
        score = metric["scores"]["mean"]
        if best is None or score > best["score"]:
            # a pattern.json's own metrics list carries several named scores
            # at once (faithfulness, answer_correctness, context_correctness,
            # answer_relevance, overall_score, confirmed live 2026-09-28
            # against a real completed run), not only the one flagged
            # optimization_metric. that one still decides the winner, but
            # the rest are worth showing too, so keep all of them here
            # rather than discarding everything except the winning score.
            best = {
                "pattern_name": body.get("name"),
                "metric_name": metric.get("name"),
                "score": score,
                "settings": body.get("settings"),
                "all_metrics": [
                    {"name": m.get("name"), "score": m.get("scores", {}).get("mean")}
                    for m in metrics
                    if m.get("scores", {}).get("mean") is not None
                ],
            }

    return {"state": state, "patterns_tested": len(pattern_keys), "winner": best}


# ---- spear shield chat, PHASE2_PLAN.md section 8 ----


def _k8s_api_ctx():
    # the real in cluster apiserver ca, not the skip verification context
    # the other helpers above use for routes with their own serving certs
    return ssl.create_default_context(cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")


def openshift_username(user_token):
    # a self lookup every token can do regardless of what rbac that user
    # otherwise holds, this is how the backend learns who is actually
    # signed into the console right now, never trust a client supplied
    # username for this
    req = urllib.request.Request(
        "https://kubernetes.default.svc/apis/user.openshift.io/v1/users/~",
        headers={"Authorization": f"Bearer {user_token}"},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=10, context=_k8s_api_ctx()) as resp:
        return json.loads(resp.read())["metadata"]["name"]


def persona_password(username):
    # one file per username, filename is the username, same shape
    # spear-openproject-mcp's own OPENPROJECT_TOKENS_DIR already uses
    path = os.path.join(SPEAR_SHIELD_CREDS_DIR, username)
    if not os.path.isfile(path):
        raise LookupError(f"no spear-shield-agents realm password on file for {username!r}")
    with open(path) as f:
        return f.read().strip()


def mint_spear_shield_token(username, password):
    data = urllib.parse.urlencode(
        {
            "grant_type": "password",
            "client_id": SPEAR_COORDINATOR_CLIENT_ID,
            "client_secret": open(SPEAR_COORDINATOR_CLIENT_SECRET_PATH).read().strip(),
            "username": username,
            "password": password,
            "scope": "openid",
        }
    ).encode()
    req = urllib.request.Request(
        f"{SPEAR_SHIELD_ISSUER_URL}/protocol/openid-connect/token", data=data, method="POST"
    )
    with urllib.request.urlopen(req, timeout=15, context=_no_verify_ctx()) as resp:
        return json.loads(resp.read())["access_token"]


def ask_spear_shield(message_text, user_token):
    username = openshift_username(user_token)
    password = persona_password(username)
    shield_token = mint_spear_shield_token(username, password)

    rpc_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "message/send",
        "params": {
            "callerToken": shield_token,
            "message": {
                "role": "user",
                "parts": [{"kind": "text", "text": message_text}],
                "messageId": f"console-{int(time.time() * 1000)}",
            },
        },
    }
    req = urllib.request.Request(
        SPEAR_COORDINATOR_A2A_URL,
        data=json.dumps(rpc_body).encode(),
        headers={
            "Content-Type": "application/json",
            # the gateway's own authpolicy needs a real bearer token here too.
            # confirmed live this header does not survive the openshell
            # relay hop into the coordinator's own sandbox, which is exactly
            # why the same token is also sent as callerToken above, that is
            # the one the coordinator's own code actually reads
            "Authorization": f"Bearer {shield_token}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=90, context=_no_verify_ctx()) as resp:
        rpc_response = json.loads(resp.read())

    if "error" in rpc_response:
        raise RuntimeError(rpc_response["error"].get("message", "spear shield returned an error"))

    result = rpc_response.get("result", {})
    artifacts = result.get("artifacts", [])
    texts = [
        part["text"]
        for artifact in artifacts
        for part in artifact.get("parts", [])
        if part.get("kind") == "text"
    ]
    answer_text = "\n\n".join(texts) if texts else "no answer came back from spear shield"
    # the coordinator's own trace list, a2a_server.py's completed_task, not
    # fabricated here: every entry is a real wall clock duration measured
    # inside the coordinator sandbox for this exact request
    return answer_text, result.get("trace") or []


# ---- architecture status + security demo, shared k8s helpers ----

_k8s_core_v1 = None


def k8s_core_v1():
    global _k8s_core_v1
    if _k8s_core_v1 is None:
        k8s_config.load_incluster_config()
        _k8s_core_v1 = k8s_client.CoreV1Api()
    return _k8s_core_v1


def k8s_exec(pod, command, timeout=25, api=None):
    # runs a real shell command inside a real sandbox pod and returns its
    # combined stdout/stderr. container is always "agent", confirmed live,
    # openshell's own sidecar containers never run this. api defaults to
    # the shared singleton below for a lone call, callers running two of
    # these concurrently must each pass their own client, see
    # _fresh_core_v1's own comment for why that is not optional there
    api = api or k8s_core_v1()
    return k8s_stream(
        api.connect_get_namespaced_pod_exec,
        pod,
        SPEAR_SHIELD_NAMESPACE,
        container="agent",
        command=["sh", "-c", command],
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=True,
        _request_timeout=timeout,
    ).strip()


def _fresh_core_v1():
    # confirmed live: two concurrent exec websockets sharing the one
    # module level CoreV1Api's connection pool corrupt each other, the
    # apiserver's own log calls it "unknown stream id, discarding
    # message" and the client side sees a bogus "handshake status 200
    # ok" instead of a real upgrade. harmless for one exec at a time, the
    # coordinator/retrieval pair below is never one at a time, so each
    # side of that pair gets its own client, never the shared singleton
    k8s_config.load_incluster_config()
    return k8s_client.CoreV1Api()


def k8s_pod_status(pod):
    p = k8s_core_v1().read_namespaced_pod(pod, SPEAR_SHIELD_NAMESPACE)
    statuses = p.status.container_statuses or []
    return {
        "name": pod,
        "phase": p.status.phase,
        "ready": bool(statuses) and all(c.ready for c in statuses),
        "runtime_class": p.spec.runtime_class_name or "runc (default)",
        "node": p.spec.node_name,
    }


def _probe(url, headers=None, method="GET", timeout=8):
    # a real, short, read only reachability probe, used by the
    # architecture status page's live badges below. any http response at
    # all, even an error one, counts as "reachable", only a connection
    # failure or timeout counts as down
    try:
        req = urllib.request.Request(url, headers=headers or {}, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=_no_verify_ctx()) as resp:
            return True, resp.status
    except urllib.error.HTTPError as exc:
        return True, exc.code
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def spear_shield_status():
    coordinator = k8s_pod_status(COORDINATOR_POD)
    retrieval = k8s_pod_status(RETRIEVAL_POD)
    keycloak_ok, keycloak_detail = _probe(f"{SPEAR_SHIELD_ISSUER_URL}/.well-known/openid-configuration")
    a2a_ok, a2a_detail = _probe(SPEAR_COORDINATOR_A2A_URL, method="POST")
    mcp_ok, mcp_detail = _probe(
        SPEAR_SHIELD_MCP_GATEWAY_URL,
        headers={"host": SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER} if SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER else {},
        method="POST",
    )
    guardrails_ok, guardrails_detail = _probe(f"{GUARDRAILS_ROUTE}/v1/chat/completions", method="POST")
    return {
        "components": [
            {
                "id": "keycloak",
                "label": "keycloak, spear-shield-agents realm",
                "ok": keycloak_ok,
                "detail": str(keycloak_detail),
            },
            {
                "id": "coordinator",
                "label": "spear-coordinator sandbox, runc",
                "ok": coordinator["phase"] == "Running" and coordinator["ready"],
                "detail": f"phase={coordinator['phase']} ready={coordinator['ready']} runtimeClass={coordinator['runtime_class']} node={coordinator['node']}",
            },
            {
                "id": "retrieval",
                "label": "spear-retrieval sandbox, kata",
                "ok": retrieval["phase"] == "Running" and retrieval["ready"],
                "detail": f"phase={retrieval['phase']} ready={retrieval['ready']} runtimeClass={retrieval['runtime_class']} node={retrieval['node']}",
            },
            {
                "id": "a2a-gateway",
                "label": "a2a gateway, spire workload identity + kuadrant authpolicy",
                "ok": a2a_ok,
                "detail": str(a2a_detail),
            },
            {
                "id": "mcp-gateway",
                "label": "mcp gateway, per tool rbac",
                "ok": mcp_ok,
                "detail": str(mcp_detail),
            },
            {
                "id": "guardrails",
                "label": "ogx guardrails rails",
                "ok": guardrails_ok,
                "detail": str(guardrails_detail),
            },
        ],
        "same_node": coordinator["node"] == retrieval["node"] and coordinator["node"] is not None,
    }


def _mcp_tool_text(response):
    # a tool level rbac rejection at the gateway and an assignee=self
    # rejection inside spear-openproject-mcp's own code land in two
    # different shapes, confirmed live: the gateway's own crafted rejection
    # is a top level json-rpc error, the mcp server's uncaught
    # PermissionError also bubbles up as a top level error (its own
    # do_POST never catches it either), a genuine success is the only path
    # that reaches result.content
    if "error" in response:
        return response["error"].get("message", json.dumps(response["error"]))
    content = (response.get("result") or {}).get("content") or []
    return content[0].get("text") if content else json.dumps(response.get("result") or response)


def mcp_tool_call(name, arguments, caller_token):
    headers = {
        "authorization": f"Bearer {caller_token}",
        "host": SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER,
        "content-type": "application/json",
        "accept": "application/json, text/event-stream",
    }
    init_body = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "owner-console-backend", "version": "1.0"},
        },
    }
    req = urllib.request.Request(
        SPEAR_SHIELD_MCP_GATEWAY_URL, data=json.dumps(init_body).encode(), headers=headers, method="POST"
    )
    with urllib.request.urlopen(req, timeout=15, context=_no_verify_ctx()) as resp:
        session_id = resp.headers.get("Mcp-Session-Id") or resp.headers.get("mcp-session-id")
    if session_id:
        headers["mcp-session-id"] = session_id

    call_body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": name, "arguments": arguments}}
    req = urllib.request.Request(
        SPEAR_SHIELD_MCP_GATEWAY_URL, data=json.dumps(call_body).encode(), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=20, context=_no_verify_ctx()) as resp:
            raw = resp.read().decode()
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
    if raw.startswith("event:"):
        for line in raw.splitlines():
            if line.startswith("data: "):
                raw = line[6:]
                break
    return json.loads(raw) if raw else {}


# ---- the under the hood demo scenarios, PHASE2_PLAN.md section 8's
# second pass: every one of these is the same live check already run by
# hand against the real cluster earlier in that session, turned into a
# repeatable console button rather than a one off curl. scenarios 7 (abac
# content scoping) and 8 (prompt injection refusal) from that session's own
# proposal need no new code at all, the existing /spear-shield/ask endpoint
# already demonstrates both live with the right question. tool response
# injection defense is deliberately left out, see that session's own
# finding: it currently fails closed on every tool response, not only
# malicious ones, the shared guardrails config was never tuned for
# structured tool output shapes. kata vs runc, network containment and
# filesystem containment used to be three separate single sandbox
# scenarios here, folded later into demo_sandbox_compare below: one real
# call that runs the same probes against both sandboxes at once and lets
# the console render them side by side instead of one terminal at a time ----


def demo_identity_rejected():
    rpc_body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "message/send",
        "params": {
            "callerToken": "not-a-real-token",
            "message": {"role": "user", "parts": [{"kind": "text", "text": "what are my open work packages"}]},
        },
    }
    req = urllib.request.Request(
        SPEAR_COORDINATOR_A2A_URL,
        data=json.dumps(rpc_body).encode(),
        headers={"Content-Type": "application/json", "Authorization": "Bearer not-a-real-token"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15, context=_no_verify_ctx()) as resp:
            status, detail = resp.status, resp.read().decode()[:300]
    except urllib.error.HTTPError as exc:
        status, detail = exc.code, exc.read().decode()[:300]
    return {
        "title": "wrong identity rejected at the a2a gateway",
        "steps": [
            {
                "label": "send a garbage bearer token straight at the coordinator's own a2a route",
                "command": f"POST {SPEAR_COORDINATOR_A2A_URL}\nAuthorization: Bearer not-a-real-token",
                "output": f"HTTP {status}\n{detail}",
                "ok": status in (401, 403),
            }
        ],
    }


def _exec_both(command, timeout=25):
    # the identical command against both real sandboxes at once, not one
    # after the other, the whole point of a side by side compare is that
    # neither side waits on the other. each side gets its own api client,
    # see _fresh_core_v1
    with ThreadPoolExecutor(max_workers=2) as pool:
        c_future = pool.submit(k8s_exec, COORDINATOR_POD, command, timeout, _fresh_core_v1())
        r_future = pool.submit(k8s_exec, RETRIEVAL_POD, command, timeout, _fresh_core_v1())
        return c_future.result(), r_future.result()


# marker printed right before each command in a batch, chosen to be
# something real command output is never going to produce by accident
_BATCH_MARK = "@@PROBE@@"


def _exec_batch(pod, commands, timeout=40, api=None):
    # every probe command against one pod inside a single real exec, not
    # a separate kubectl exec round trip per probe, the six real probes
    # below used to cost six api server round trips each sandbox, now it
    # is one script with the marker printed ahead of every command so
    # the combined output can still be split back apart per probe
    script = "".join(f'echo "{_BATCH_MARK}{i}"\n{cmd}\n' for i, cmd in enumerate(commands))
    raw = k8s_exec(pod, script, timeout, api=api)
    outputs = [""] * len(commands)
    for chunk in raw.split(_BATCH_MARK)[1:]:
        index, _, output = chunk.partition("\n")
        if index.strip().isdigit():
            outputs[int(index)] = output.strip()
    return outputs


def _exec_both_batch(commands, timeout=40):
    # same lockstep parallel pattern as _exec_both, just carrying every
    # probe for a sandbox in one shot instead of one call per probe, each
    # side still gets its own api client, see _fresh_core_v1
    with ThreadPoolExecutor(max_workers=2) as pool:
        c_future = pool.submit(_exec_batch, COORDINATOR_POD, commands, timeout, _fresh_core_v1())
        r_future = pool.submit(_exec_batch, RETRIEVAL_POD, commands, timeout, _fresh_core_v1())
        return c_future.result(), r_future.result()


def _reachable(curl_output):
    # a real http_code came back at all, not a proxy level connection
    # refusal (http_code=000 or no http_code token means the request
    # never got a response, blocked or unreachable either way)
    return "http_code=" in curl_output and "http_code=000" not in curl_output


def _annotate_blocked(curl_output):
    # curl's own placeholder for "no http response ever came back", not a
    # real status code the destination returned. confirmed live: this
    # always means this sandbox's own openshell sidecar proxy refused the
    # connect outright because the destination is not on that sandbox's
    # own egress allow list, surfaced plainly here so a 000 reads as a
    # policy decision, not a mystery error
    if "http_code=000" in curl_output:
        return curl_output + "\nblocked before any response: destination is not on this sandbox's own egress allow list"
    return curl_output


def _egress_probe_cmd(url):
    return (
        SANDBOX_PROXY_CURL
        + " --cacert /etc/openshell-tls/proxy/ca-bundle.pem"
        + f' -o /dev/null -w "http_code=%{{http_code}}" "{url}"'
    )


def _probe_result(probe_id, label, command, c_output, r_output, signal_fn=lambda o: o):
    return {
        "id": probe_id,
        "label": label,
        "command": command,
        "coordinator_output": c_output,
        "retrieval_output": r_output,
        "verdict": "same" if signal_fn(c_output) == signal_fn(r_output) else "differs",
    }


def _group_runtime_isolation(coordinator, retrieval):
    kernel_c, kernel_r = _exec_both("uname -r")
    return {
        "id": "runtime-isolation",
        "label": "Runtime Isolation",
        "probes": [
            _probe_result(
                "runtime-class",
                "which runtime is this pod",
                "oc get pod {pod} -o jsonpath='{.spec.runtimeClassName}'",
                coordinator["runtime_class"],
                retrieval["runtime_class"],
            ),
            _probe_result("uname", "uname -r", "uname -r", kernel_c, kernel_r),
        ],
    }


def _group_resource_visibility(_coordinator, _retrieval):
    # confirmed live: runc shares the host kernel so nproc/free report
    # whatever the host itself has (32 cores, 125gi here), kata boots its
    # own real micro vm with its own kernel so nproc/free report only what
    # that vm was actually given (1 core, under 2gi), this is expected to
    # differ, the point of this probe is showing that each runtime
    # accounts cpu/memory at a different boundary, not that they match
    footing_c, footing_r = _exec_both("echo cpus=$(nproc); free -h")
    return {
        "id": "resource-visibility",
        "label": "Resource Visibility",
        "probes": [_probe_result("cpu-mem", "nproc + free -h", "nproc; free -h", footing_c, footing_r)],
    }


def _group_common_containment(_coordinator, _retrieval):
    # two real probes, one exec per pod rather than two, see
    # _exec_batch's own header comment for why
    commands = [
        f'{SANDBOX_PROXY_CURL} -o /dev/null -w "http_code=%{{http_code}}" https://1.1.1.1',
        "rm -f /usr/bin/ls /usr/bin/cat 2>&1; echo exit=$?",
    ]
    coordinator_out, retrieval_out = _exec_both_batch(commands)
    host_c, rm_c = coordinator_out
    host_r, rm_r = retrieval_out
    return {
        "id": "common-containment",
        "label": "Common Containment",
        "probes": [
            _probe_result(
                "arbitrary-host",
                "curl an arbitrary internet host",
                "curl -x 127.0.0.1:3128 https://1.1.1.1",
                _annotate_blocked(host_c),
                _annotate_blocked(host_r),
                _reachable,
            ),
            _probe_result(
                "delete-binaries",
                "delete real system binaries",
                "rm -f /usr/bin/ls /usr/bin/cat",
                rm_c,
                rm_r,
                lambda o: "Permission denied" in o,
            ),
        ],
    }


def _group_least_privilege_egress(_coordinator, _retrieval):
    commands = [
        _egress_probe_cmd(f"{GUARDRAILS_ROUTE}/v1/chat/completions"),
        _egress_probe_cmd(f"{RAG_QUERY_RELAY_URL}/healthz"),
    ]
    coordinator_out, retrieval_out = _exec_both_batch(commands)
    guardrails_c, relay_c = coordinator_out
    guardrails_r, relay_r = retrieval_out
    return {
        "id": "least-privilege-egress",
        "label": "Least Privilege Egress",
        "probes": [
            _probe_result(
                "curl-guardrails",
                "curl the guardrails route",
                f'curl -x 127.0.0.1:3128 "{GUARDRAILS_ROUTE}/v1/chat/completions"',
                _annotate_blocked(guardrails_c),
                _annotate_blocked(guardrails_r),
                _reachable,
            ),
            _probe_result(
                "curl-rag-relay",
                "curl the rag-query-relay route",
                f'curl -x 127.0.0.1:3128 "{RAG_QUERY_RELAY_URL}/healthz"',
                _annotate_blocked(relay_c),
                _annotate_blocked(relay_r),
                _reachable,
            ),
        ],
    }


# declared in display order, a run all walks this same order so a single
# group run and a run all land identically either way
COMPARE_GROUP_IDS = ["runtime-isolation", "resource-visibility", "common-containment", "least-privilege-egress"]
_COMPARE_GROUP_BUILDERS = {
    "runtime-isolation": _group_runtime_isolation,
    "resource-visibility": _group_resource_visibility,
    "common-containment": _group_common_containment,
    "least-privilege-egress": _group_least_privilege_egress,
}


def demo_sandbox_identity():
    # just the two real pods' own identity, no exec involved, cheap
    # enough to call the moment the page loads so both terminals show who
    # they really are before anyone presses a single run button
    coordinator = k8s_pod_status(COORDINATOR_POD)
    retrieval = k8s_pod_status(RETRIEVAL_POD)
    return {
        "namespace": SPEAR_SHIELD_NAMESPACE,
        "coordinator": {"pod": COORDINATOR_POD, **coordinator},
        "retrieval": {"pod": RETRIEVAL_POD, **retrieval},
    }


def demo_sandbox_compare(group_id=None):
    # the whole "two runtimes, side by side" page in one call when
    # group_id is empty, or just the one group whose own run button was
    # pressed instead of the top level run all. every probe below runs
    # for real against both sandboxes, in parallel, and the verdict
    # (same/differs) is computed live from the two real outputs, never a
    # pre labeled assumption. a probe landing as "differs" is not
    # automatically a problem, kata vs runc is supposed to differ, a
    # policy asymmetry is supposed to differ, this just reports what
    # actually happened on both sides
    coordinator = k8s_pod_status(COORDINATOR_POD)
    retrieval = k8s_pod_status(RETRIEVAL_POD)
    group_ids = [group_id] if group_id else COMPARE_GROUP_IDS
    groups = [_COMPARE_GROUP_BUILDERS[g](coordinator, retrieval) for g in group_ids]
    return {
        "namespace": SPEAR_SHIELD_NAMESPACE,
        "coordinator": {"pod": COORDINATOR_POD, **coordinator},
        "retrieval": {"pod": RETRIEVAL_POD, **retrieval},
        "groups": groups,
    }


def demo_exec_both_command(command):
    # the free typed companion to the groups above, same two pods, same
    # parallel exec, whatever is typed runs for real against both at once
    c_output, r_output = _exec_both(command)
    return {"command": command, "coordinator_output": c_output, "retrieval_output": r_output}


def demo_workload_identity():
    # a pass/fail contrast, not a literal claims readout. confirmed live
    # this session that reading the retrieval agent's own inbound
    # authorization header is not a usable proof here: openshell's own
    # a2a router strips any caller presented authorization header before
    # it reaches the sandboxed app, for every caller, a sound anti
    # spoofing property since a2a itself has no per hop auth of its own.
    # so instead this shows the same naked call made two ways: from
    # outside any sandbox with no token at all, which the retrieval a2a
    # gateway's own authpolicy cleanly rejects, versus the real
    # coordinator process making the identical call with no token of its
    # own, which succeeds, proving something transparently attached a
    # valid credential before that request ever left the coordinator pod
    naked_body = {"jsonrpc": "2.0", "id": 1, "method": "whoami", "params": {}}
    naked_req = urllib.request.Request(
        RETRIEVAL_A2A_INTERNAL_URL,
        data=json.dumps(naked_body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(naked_req, timeout=15, context=_no_verify_ctx()) as resp:
            naked_status, naked_detail = resp.status, resp.read().decode()[:300]
    except urllib.error.HTTPError as exc:
        naked_status, naked_detail = exc.code, exc.read().decode()[:300]

    # the a2a gateway's own authpolicy (scenario 1's own point) still
    # gates this outer hop before it ever reaches the coordinator's
    # dispatch, a real token is needed here too, whose persona does not
    # matter since whoami-downstream itself checks no caller identity
    gateway_token = mint_spear_shield_token("balakrishnan.b", persona_password("balakrishnan.b"))
    downstream_body = {"jsonrpc": "2.0", "id": 1, "method": "whoami-downstream", "params": {}}
    downstream_req = urllib.request.Request(
        SPEAR_COORDINATOR_A2A_URL,
        data=json.dumps(downstream_body).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {gateway_token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(downstream_req, timeout=20, context=_no_verify_ctx()) as resp:
            outer = json.loads(resp.read())
        inner_text = outer["result"]["artifacts"][0]["parts"][0]["text"]
        downstream_succeeded = json.loads(inner_text).get("result", {}).get("artifacts", [{}])[0].get(
            "parts", [{}]
        )[0].get("text") == json.dumps({"reached": True})
        downstream_detail = inner_text
    except Exception as exc:  # noqa: BLE001
        downstream_succeeded, downstream_detail = False, str(exc)

    return {
        "title": "workload identity, this hop authenticates as this exact sandbox, not a shared secret",
        "steps": [
            {
                "label": "the same call, made from outside any sandbox, with no token at all",
                "command": f"POST {RETRIEVAL_A2A_INTERNAL_URL} (no Authorization header)",
                "output": f"HTTP {naked_status}\n{naked_detail}",
                "ok": naked_status in (401, 403),
            },
            {
                "label": "the real coordinator process makes the identical call, also with no token of its own",
                "command": f"http_json({RETRIEVAL_A2A_INTERNAL_URL}, whoami), the exact channel every real tool delegation already uses",
                "output": (
                    "this call succeeded where the naked call above was rejected, the openshell supervisor "
                    "transparently attached a valid credential minted for this exact sandbox before the "
                    f"request ever left the pod:\n{downstream_detail}"
                ),
                "ok": downstream_succeeded,
            },
        ],
    }


def demo_tool_scope_and_assignee():
    bala_token = mint_spear_shield_token("balakrishnan.b", persona_password("balakrishnan.b"))
    sid_token = mint_spear_shield_token("siddhartha.de", persona_password("siddhartha.de"))

    # #38 is really assigned to balakrishnan.b, #39 to siddhartha.de,
    # PHASE2_PLAN.md section 4's own real openproject data, not invented ids
    step1 = _mcp_tool_text(mcp_tool_call("openproject_update_work_package", {"id": 38, "status": "in progress"}, bala_token))
    step2 = _mcp_tool_text(mcp_tool_call("openproject_update_work_package", {"id": 38, "status": "in progress"}, sid_token))
    step3 = _mcp_tool_text(mcp_tool_call("openproject_update_work_package", {"id": 39, "status": "in progress"}, sid_token))

    return {
        "title": "two layers, three real callers: gateway tool rbac, then the project's own assignee=self check",
        "steps": [
            {
                "label": "balakrishnan.b (search only role) tries update_work_package on #38, his own item",
                "command": "tools/call openproject_update_work_package {id: 38} as balakrishnan.b",
                "output": step1,
                "ok": "forbidden" in step1.lower() or "insufficient" in step1.lower(),
            },
            {
                "label": "siddhartha.de (holds the update role) tries update_work_package on #38, balakrishnan's own item",
                "command": "tools/call openproject_update_work_package {id: 38} as siddhartha.de",
                "output": step2,
                "ok": "not assigned to siddhartha.de" in step2,
            },
            {
                "label": "siddhartha.de updates #39, her own item, genuinely succeeds",
                "command": "tools/call openproject_update_work_package {id: 39} as siddhartha.de",
                "output": step3,
                "ok": "not assigned" not in step3 and "forbidden" not in step3.lower(),
            },
        ],
    }


DEMO_SCENARIOS = {
    "identity-rejected": demo_identity_rejected,
    "sandbox-compare": demo_sandbox_compare,
    "workload-identity": demo_workload_identity,
    "tool-scope-and-assignee": demo_tool_scope_and_assignee,
}


class Handler(BaseHTTPRequestHandler):
    def _json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _raw(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _user_token(self):
        auth = self.headers.get("Authorization", "")
        if not auth.lower().startswith("bearer "):
            raise PermissionError(
                "No forwarded user token, is the proxy alias set to authorization: UserToken?"
            )
        return auth.split(" ", 1)[1].strip()

    def _query(self):
        return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(self.path).query))

    def do_GET(self):
        path = urllib.parse.urlsplit(self.path).path
        query = self._query()
        try:
            if path == "/healthz":
                self._json(200, {"status": "ok"})
            elif path == "/runs":
                self._json(200, {"runs": merged_ingestion_runs(self._user_token())})
            elif path == "/report":
                self._json(200, ingestion_report(query.get("run_id")))
            elif path == "/kfp/ingestion-runs":
                self._json(200, {"runs": recent_kfp_runs(INGESTION_PIPELINE_NAME, 10, self._user_token())})
            elif path == "/kfp/autorag-runs":
                self._json(200, {"runs": recent_kfp_runs(AUTORAG_PIPELINE_NAME, 10, self._user_token())})
            elif path == "/kfp/apply-pattern-runs":
                self._json(200, {"runs": recent_kfp_runs(APPLY_PATTERN_PIPELINE_NAME, 10, self._user_token())})
            elif path == "/kfp/autorag-leaderboard":
                run_id = query.get("run_id")
                if not run_id:
                    self._json(400, {"error": "run_id query param is required"})
                else:
                    self._json(200, autorag_leaderboard(run_id, self._user_token()))
            elif path == "/ingestion/source-config":
                # read-only view of where sanitization reads its sources
                # from, the knowledge base page's source strip. no secret
                # name, no credential material, see ingestion_source_config
                # and the comment on the INGESTION_SOURCE_* constants above
                self._json(200, ingestion_source_config(self._user_token()))
            elif path == "/spear-shield/status":
                self._json(200, spear_shield_status())
            elif path == "/spear-shield/sandbox-identity":
                self._json(200, demo_sandbox_identity())
            elif path == "/trend/eval-cycles":
                limit = int(query.get("limit", 10))
                self._json(200, {"cycles": recent_eval_cycles(limit)})
            elif path == "/trend/artifact":
                run_id = query.get("run_id", "")
                artifact_path = query.get("path", "")
                # ".." is rejected outright rather than relied on mlflow's
                # own server side scoping, no reason to forward anything
                # that looks like a path escape attempt at all
                if not run_id or not artifact_path or ".." in artifact_path:
                    self._json(400, {"error": "run_id and path query params are required"})
                else:
                    content_type = "text/html" if artifact_path.endswith(".html") else "application/octet-stream"
                    self._raw(200, mlflow_get_artifact(run_id, artifact_path), content_type)
            else:
                self._json(404, {"error": "Not found"})
        except PermissionError as exc:
            self._json(401, {"error": str(exc)})
        except Exception as exc:  # noqa: BLE001 - always report as json, never a bare 500 page
            self._json(502, {"error": str(exc)})

    def do_POST(self):
        path = urllib.parse.urlsplit(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {}
        try:
            if path == "/trigger/ingestion":
                # a fixed fallback batch id used to collide with itself on a
                # second click with the field left blank, both mlflow's
                # run_name and the merged run picker key on batch_id, so two
                # such runs would have hidden each other. a timestamp makes
                # every auto generated batch id unique.
                batch_id = body.get("batch_id") or f"console-{int(time.time())}"
                result = trigger_kfp_run(
                    INGESTION_PIPELINE_NAME,
                    INGESTION_KFP_EXPERIMENT_NAME,
                    f"{INGESTION_PIPELINE_NAME}-{batch_id}",
                    {
                        "batch_id": batch_id,
                        "guardrails_route": GUARDRAILS_ROUTE,
                        "mlflow_url": MLFLOW_URL,
                    },
                    self._user_token(),
                )
                result["batch_id"] = batch_id
                result["dashboard_url"] = dashboard_run_link(result["run_id"])
                self._json(200, result)
            elif path == "/trigger/autorag":
                batch_id = body.get("batch_id")
                if not batch_id:
                    self._json(
                        400,
                        {
                            "error": "batch_id is required, must match the sanitization "
                            "batch that produced the eval dataset it will score against"
                        },
                    )
                    return
                params = {
                    "embedding_models": AUTORAG_EMBEDDING_MODELS,
                    "generation_models": AUTORAG_GENERATION_MODELS,
                    "input_data_bucket_name": AUTORAG_RAW_BUCKET,
                    "input_data_key": "sanitized/",
                    "input_data_secret_name": AUTORAG_S3_SECRET_NAME,
                    "ogx_secret_name": AUTORAG_OGX_SECRET_NAME,
                    "vector_io_provider_id": AUTORAG_VECTOR_IO_PROVIDER_ID,
                    "optimization_max_rag_patterns": AUTORAG_MAX_RAG_PATTERNS,
                    "optimization_metric": AUTORAG_OPTIMIZATION_METRIC,
                    "preset": AUTORAG_PRESET,
                    "test_data_bucket_name": AUTORAG_RAW_BUCKET,
                    "test_data_key": f"autorag-eval/{batch_id}_test_data.json",
                    "test_data_secret_name": AUTORAG_S3_SECRET_NAME,
                }
                result = trigger_kfp_run(
                    AUTORAG_PIPELINE_NAME,
                    AUTORAG_KFP_EXPERIMENT_NAME,
                    f"{AUTORAG_PIPELINE_NAME}-{batch_id}",
                    params,
                    self._user_token(),
                )
                result["batch_id"] = batch_id
                result["dashboard_url"] = dashboard_run_link(result["run_id"])
                self._json(200, result)
            elif path == "/trigger/apply-pattern":
                batch_id = body.get("batch_id")
                autorag_run_id = body.get("autorag_run_id")
                if not batch_id or not autorag_run_id:
                    self._json(
                        400,
                        {"error": "batch_id and autorag_run_id are both required"},
                    )
                    return
                user_token = self._user_token()
                leaderboard = autorag_leaderboard(autorag_run_id, user_token)
                winner = leaderboard.get("winner")
                if winner is None:
                    self._json(
                        400,
                        {
                            "error": f"Run {autorag_run_id!r} has no winning pattern yet "
                            f"(state: {leaderboard.get('state')}), nothing to apply"
                        },
                    )
                    return
                settings = winner["settings"]
                params = {
                    "mlflow_url": MLFLOW_URL,
                    "batch_id": batch_id,
                    "pattern_name": winner["pattern_name"],
                    "autorag_run_id": autorag_run_id,
                    "embedding_model": settings["embedding"]["model_id"],
                    "chunk_size": settings["chunking"]["chunk_size"],
                    "chunk_overlap": settings["chunking"]["chunk_overlap"],
                }
                result = trigger_kfp_run(
                    APPLY_PATTERN_PIPELINE_NAME,
                    APPLY_PATTERN_KFP_EXPERIMENT_NAME,
                    f"{APPLY_PATTERN_PIPELINE_NAME}-{batch_id}",
                    params,
                    user_token,
                )
                result["batch_id"] = batch_id
                result["pattern_name"] = winner["pattern_name"]
                result["dashboard_url"] = dashboard_run_link(result["run_id"])
                self._json(200, result)
            elif path == "/spear-shield/ask":
                message_text = (body.get("message") or "").strip()
                if not message_text:
                    self._json(400, {"error": "message is required"})
                    return
                answer, trace = ask_spear_shield(message_text, self._user_token())
                self._json(200, {"answer": answer, "trace": trace})
            elif path == "/spear-shield/demo-exec-both":
                command = (body.get("command") or "").strip()
                if not command:
                    self._json(400, {"error": "command is required"})
                    return
                self._json(200, demo_exec_both_command(command))
            elif path.startswith("/spear-shield/demo/"):
                scenario_id = path[len("/spear-shield/demo/"):]
                scenario_fn = DEMO_SCENARIOS.get(scenario_id)
                if scenario_fn is None:
                    self._json(404, {"error": f"unknown demo scenario: {scenario_id!r}"})
                    return
                if scenario_id == "sandbox-compare":
                    # an empty group runs every group, same as always, a
                    # named one runs just the group whose own button was
                    # pressed instead of the top level run all
                    group_id = (body.get("group") or "").strip() or None
                    if group_id and group_id not in COMPARE_GROUP_IDS:
                        self._json(404, {"error": f"unknown compare group: {group_id!r}"})
                        return
                    self._json(200, scenario_fn(group_id))
                else:
                    self._json(200, scenario_fn())
            else:
                self._json(404, {"error": "Not found"})
        except PermissionError as exc:
            self._json(401, {"error": str(exc)})
        except LookupError as exc:
            self._json(403, {"error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._json(502, {"error": str(exc)})

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args))


if __name__ == "__main__":
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile="/etc/tls/private/tls.crt", keyfile="/etc/tls/private/tls.key")
    server = ThreadingHTTPServer(("0.0.0.0", 8443), Handler)
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    server.serve_forever()
