"""phase1-apply-pattern-pipeline: the step that used to happen too early.

before 2026-09-28 phase1_ingestion_pipeline chunked, embedded and wrote
every sanitized document into rag_chunks itself, with a fixed size
placeholder chunker, before autorag ever got a chance to pick a real
winning recipe against the eval set that same pipeline produced. autorag's
own leaderboard winner was purely informational, never actually applied
to the production index, which is backwards relative to how autorag is
meant to be used: run the optimization sweep first, then index the full
corpus with the winning recipe's exact settings. see PHASE1_PLAN.md for
the full writeup.

this pipeline is that second half, now decoupled and run after the fact.
given a batch_id (to find that batch's already logged ingestion report,
for the document list and abac tags) and a winning pattern's settings
(chunk_size, chunk_overlap and embedding_model_id, passed in as plain
run parameters, already extracted by the caller from a completed autorag
run's pattern.json, see owner-console-backend/server.py's
autorag_leaderboard()), this pipeline re chunks and re embeds every
document that batch's own guardrails audit marked "success", deletes any
rows rag_chunks already had for those doc_ids first (chunk boundaries
move when chunk_size/overlap change, so old rows would otherwise become
orphaned), and writes fresh rows tagged with the pattern_name and
autorag_run_id that produced them, so which recipe indexed a given row is
always visible, not silently overwritten with no trace.

honest limitation, disclosed rather than hidden: autorag's own winning
pattern can name a chunking method other than a plain fixed size window
(a real completed run on this cluster picked "recursive"), and this
pipeline only ever reproduces chunk_size and chunk_overlap, not
ai4rag's own recursive splitting algorithm, which is not something this
project can inspect or reproduce. the size and overlap and the embedding
model are the winner's real settings, the splitting method itself is
still the same fixed window slicer phase1_ingestion_pipeline used to
carry, just parameterized now instead of hardcoded.

run `python3 pipeline.py` to compile phase1-apply-pattern-pipeline.yaml,
or `python3 compile_and_run.py <batch_id> <autorag_run_id>` to also
upload and trigger a real run against rag-phase1-dspa.
"""
from typing import NamedTuple

from kfp import dsl
from kfp import kubernetes

BASE_IMAGE = "registry.access.redhat.com/ubi9/python-311:latest"

MINIO_ENDPOINT_DEFAULT = "minio.minio.svc.cluster.local:9000"
RAW_BUCKET_DEFAULT = "rag-documents"
SANITIZED_PREFIX_DEFAULT = "sanitized/"
OGX_BASE_URL_DEFAULT = "http://rag-phase1-ogx-service.spear-inference.svc.cluster.local:8321"
MLFLOW_WORKSPACE_DEFAULT = "spear-pipelines"
INGESTION_EXPERIMENT_NAME_DEFAULT = "phase1-ingestion"
APPLY_EXPERIMENT_NAME_DEFAULT = "phase1-apply-pattern"


@dsl.component(
    base_image=BASE_IMAGE,
    packages_to_install=["boto3==1.35.99", "psycopg2-binary==2.9.10", "requests==2.32.3"],
)
def apply_winning_pattern(
    minio_endpoint: str,
    bucket: str,
    sanitized_prefix: str,
    pgvector_host: str,
    pgvector_port: str,
    pgvector_db: str,
    ogx_base_url: str,
    mlflow_url: str,
    mlflow_workspace: str,
    ingestion_experiment_name: str,
    batch_id: str,
    pattern_name: str,
    autorag_run_id: str,
    embedding_model: str,
    chunk_size: int,
    chunk_overlap: int,
    apply_report: dsl.Output[dsl.Dataset],
) -> NamedTuple("Outputs", [("chunks_created", int), ("documents_indexed", int), ("embedding_failures", int)]):
    """fetches batch_id's own already logged ingestion_report.json from
    mlflow (the same artifact owner-console-backend/server.py's
    ingestion_report() reads), pulls the abac tags and the guardrails
    verdict per document from it, then re indexes every document that
    passed sanitization using the winning pattern's real chunk_size,
    chunk_overlap and embedding_model_id.
    """
    import hashlib
    import json
    import os
    import time

    import boto3
    import psycopg2
    import requests

    headers = {"X-MLflow-Workspace": mlflow_workspace, "Content-Type": "application/json"}
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    if os.path.exists(token_path):
        with open(token_path) as f:
            headers["Authorization"] = f"Bearer {f.read().strip()}"

    search = requests.post(
        f"{mlflow_url}/api/2.0/mlflow/experiments/search",
        headers=headers, json={"max_results": 100}, verify=False, timeout=30,
    ).json()
    exp = next((e for e in search.get("experiments", []) if e["name"] == ingestion_experiment_name), None)
    if exp is None:
        raise RuntimeError(f"no {ingestion_experiment_name!r} experiment found, has any batch ever been ingested?")

    runs = requests.post(
        f"{mlflow_url}/api/2.0/mlflow/runs/search",
        headers=headers,
        json={
            "experiment_ids": [exp["experiment_id"]],
            "filter": f"tags.mlflow.runName = '{batch_id}'",
            "max_results": 1,
        },
        verify=False, timeout=30,
    ).json().get("runs", [])
    if not runs:
        raise RuntimeError(f"no ingestion run found for batch_id {batch_id!r}, was it ever triggered?")
    ingestion_run_id = runs[0]["info"]["run_id"]

    report_resp = requests.get(
        f"{mlflow_url}/get-artifact?path=ingestion_report.json&run_uuid={ingestion_run_id}",
        headers=headers, verify=False, timeout=30,
    )
    report_resp.raise_for_status()
    report = report_resp.json()

    docs_by_id = {d["doc_id"]: d for d in report.get("valid_documents", [])}
    sanitized_rows = [r for r in report.get("guardrails", {}).get("audit", []) if r["verdict"] == "success"]

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{minio_endpoint}",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    conn = psycopg2.connect(
        host=pgvector_host, port=pgvector_port, dbname=pgvector_db,
        user=os.environ["PGVECTOR_USER"], password=os.environ["PGVECTOR_PASSWORD"],
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rag_chunks (
            id BIGSERIAL PRIMARY KEY,
            doc_id TEXT NOT NULL,
            chunk_index INT NOT NULL,
            content TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,
            source TEXT,
            clearance_level TEXT,
            dept TEXT,
            team TEXT,
            project_scope TEXT,
            data_owner TEXT,
            embedding vector(768),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (doc_id, chunk_index)
        )
        """
    )
    # provenance columns, added once, so every row always shows which
    # pattern indexed it rather than silently overwriting that history
    for column, coltype in (
        ("pattern_name", "TEXT"), ("autorag_run_id", "TEXT"),
        ("chunk_size", "INT"), ("chunk_overlap", "INT"),
    ):
        cur.execute(f"ALTER TABLE rag_chunks ADD COLUMN IF NOT EXISTS {column} {coltype}")

    # row level security, same table this function already owns the
    # schema of. the real abac decision used to live only in
    # rag_query.py's own WHERE clause, one layer, a bug there or a future
    # code path that forgets to call it would have seen every row. this
    # is the second, independent layer: rag_reader (the only role
    # rag-query-relay ever connects as) has no bypassrls, so a row only
    # ever comes back if this policy itself says so, regardless of what
    # sql runs against it. rag_reader is created by
    # scripts/03-install-data.sh, not here, a grant against a role that
    # does not exist yet should never be what blocks a real indexing run
    #
    # correction, 2026-10-11: this used to be one cur.execute() of a
    # dollar-quoted DO $$ ... $$ block doing the role check in plpgsql.
    # confirmed live, this project's first ever real apply-pattern run:
    # psycopg2.errors.SyntaxError: syntax error at or near "$", the
    # submitted sql's own DO $$ had lost one of its two $ characters by
    # the time postgres saw it. not a bug in this file: both the locally
    # compiled phase1-apply-pattern-pipeline.yaml and the exact version
    # string stored on rag-phase1-dspa's own pipeline server (fetched
    # live via the v2beta1 api and diffed byte for byte) still had the
    # correct DO $$, intact, so whatever mangles it happens inside
    # rhoai's own kfp v2 launcher when it reconstructs the component's
    # python source inside the pod at runtime, not anything compiled or
    # stored here, and not something this repo can inspect or patch, it
    # ships inside the vendored operator image. the real fix is this:
    # the role check moved into python, so no dollar-quoted plpgsql
    # block is ever sent at all, sidestepping whatever that launcher bug
    # is rather than chasing it
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'rag_reader'")
    if cur.fetchone():
        cur.execute("GRANT SELECT ON rag_chunks TO rag_reader")

    cur.execute(
        """
        ALTER TABLE rag_chunks ENABLE ROW LEVEL SECURITY;
        ALTER TABLE rag_chunks FORCE ROW LEVEL SECURITY;

        DROP POLICY IF EXISTS rag_chunks_abac ON rag_chunks;
        -- every claim-derived value is NULLIF'd to '', not just team: an
        -- unrecognized caller_clearance makes allowed_clearance_levels()
        -- return [] (rag_query.py), and a missing caller_dept is coerced
        -- to "" (caller_dept or ""), so without this normalization both
        -- current_setting() calls would come back as the literal empty
        -- string rather than NULL, and would then match any row whose
        -- own clearance_level/dept column was also stored as "" rather
        -- than NULL, fail-open for exactly the "forgot a claim" case
        -- this second, independent layer exists to survive. NULLIF makes
        -- an absent claim compare against NULL instead, which is never
        -- true, so the row is denied rather than matched.
        CREATE POLICY rag_chunks_abac ON rag_chunks
            FOR SELECT
            USING (
                clearance_level = ANY(string_to_array(NULLIF(current_setting('app.allowed_levels', true), ''), ','))
                AND (
                    dept = NULLIF(current_setting('app.dept', true), '')
                    OR team = NULLIF(current_setting('app.team', true), '')
                    OR project_scope = 'enterprise-wide'
                )
            )
        """
    )

    def embed_chunks(texts: list[str]) -> list[list[float]]:
        resp = requests.post(
            f"{ogx_base_url}/v1/embeddings",
            json={"model": embedding_model, "input": texts},
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        return [row["embedding"] for row in sorted(data, key=lambda r: r["index"])]

    def embed_chunks_with_retry(texts: list[str], attempts: int = 3) -> list[list[float]]:
        # a null embedding row can never be found by any vector search
        # again, ever, regardless of who is asking, so a transient blip
        # is worth a couple retries rather than writing a dead row into a
        # live rag index. this is the real fix for the doc-001/doc-002
        # null embedding incident, found and repaired live on 2026-10-02:
        # both rows turned out to predate this pipeline entirely, from an
        # older ingestion path with the same swallow-and-null behavior,
        # never caught because nothing downstream ever read the old
        # embedding_failures counter
        last_exc = None
        for attempt in range(attempts):
            try:
                return embed_chunks(texts)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < attempts - 1:
                    time.sleep(2 ** attempt)
        raise last_exc

    total_chunks = 0
    documents_indexed = 0
    # per doc_id, not just a count: a bare number nobody downstream reads
    # is not a real safety net, this list is what actually ends up in
    # apply_report.json and is small enough to always include in full
    embedding_failures = []
    for row in sanitized_rows:
        doc = docs_by_id.get(row["doc_id"])
        if doc is None:
            print(f"  {row['doc_id']}: no abac tags found in ingestion report, skipping")
            continue
        content = s3.get_object(
            Bucket=bucket, Key=f"{sanitized_prefix}{row['filename']}"
        )["Body"].read().decode("utf-8")

        chunks = []
        start = 0
        while start < len(content):
            chunks.append(content[start:start + chunk_size])
            start += chunk_size - chunk_overlap

        try:
            embeddings = embed_chunks_with_retry(chunks)
        except requests.RequestException as exc:
            # embed before delete: whatever rows this doc_id already has
            # from a previous successful run stay exactly as they are,
            # a failed embed here must never make a previously working
            # doc worse off. skip the doc entirely rather than writing a
            # permanently unsearchable null vector row
            print(f"  {doc['doc_id']}: embedding failed after {3} attempts ({exc}), leaving existing rows untouched, skipping")
            embedding_failures.append({"doc_id": doc["doc_id"], "error": str(exc)})
            continue

        # only now that the new embeddings are actually in hand: chunk
        # boundaries move whenever chunk_size/overlap change, so any rows
        # this doc had under a previous pattern are stale, drop them
        # before writing the fresh ones rather than leaving orphaned tails
        cur.execute("DELETE FROM rag_chunks WHERE doc_id = %s", (doc["doc_id"],))

        for idx, chunk in enumerate(chunks):
            vector_literal = "[" + ",".join(repr(v) for v in embeddings[idx]) + "]"
            cur.execute(
                """
                INSERT INTO rag_chunks
                    (doc_id, chunk_index, content, content_sha256, source,
                     clearance_level, dept, team, project_scope, data_owner,
                     embedding, pattern_name, autorag_run_id, chunk_size, chunk_overlap)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector, %s, %s, %s, %s)
                """,
                (
                    doc["doc_id"], idx, chunk, hashlib.sha256(chunk.encode("utf-8")).hexdigest(),
                    doc.get("source"), doc.get("clearance_level"), doc.get("dept"),
                    doc.get("team"), doc.get("project_scope"), doc.get("data_owner"), vector_literal,
                    pattern_name, autorag_run_id, chunk_size, chunk_overlap,
                ),
            )
        total_chunks += len(chunks)
        documents_indexed += 1
        print(f"  {doc['doc_id']}: {len(chunks)} chunks written, embedded with {embedding_model}")

    report_out = {
        "batch_id": batch_id,
        "pattern_name": pattern_name,
        "autorag_run_id": autorag_run_id,
        "table": "rag_chunks",
        "chunking_method": "fixed_size_window (winner's real chunk_size/overlap, not autorag's own splitting algorithm, see this file's module docstring)",
        "chunk_size": chunk_size,
        "chunk_overlap": chunk_overlap,
        "embedding_model": embedding_model,
        # list of {doc_id, error}, not a count: an empty list here is the
        # only thing that actually means every sanitized doc got indexed
        # and is really searchable, a doc_id in this list was left alone,
        # not written with a dead row
        "embedding_failures": embedding_failures,
        "documents_indexed": documents_indexed,
        "chunks_created": total_chunks,
    }
    with open(apply_report.path, "w") as f:
        json.dump(report_out, f, indent=2)

    cur.close()
    conn.close()

    outputs = NamedTuple("Outputs", [("chunks_created", int), ("documents_indexed", int), ("embedding_failures", int)])
    return outputs(total_chunks, documents_indexed, len(embedding_failures))


@dsl.component(base_image=BASE_IMAGE, packages_to_install=["boto3==1.35.99", "requests==2.32.3"])
def log_apply_report(
    mlflow_url: str,
    mlflow_workspace: str,
    apply_experiment_name: str,
    batch_id: str,
    pattern_name: str,
    autorag_run_id: str,
    minio_endpoint: str,
    artifact_bucket: str,
    chunks_created: int,
    documents_indexed: int,
    embedding_failures: int,
    apply_report: dsl.Input[dsl.Dataset],
) -> str:
    """logs one mlflow run per apply, under its own phase1-apply-pattern
    experiment rather than mixed into phase1-ingestion, since applying a
    pattern is a separate real world action from ingesting a batch and
    can happen more than once against the same batch as autorag keeps
    finding better recipes.
    """
    import json
    import os
    import time

    import boto3
    import requests

    with open(apply_report.path) as f:
        report = json.load(f)

    headers = {"X-MLflow-Workspace": mlflow_workspace, "Content-Type": "application/json"}
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    if os.path.exists(token_path):
        with open(token_path) as f:
            headers["Authorization"] = f"Bearer {f.read().strip()}"

    search = requests.post(
        f"{mlflow_url}/api/2.0/mlflow/experiments/search",
        headers=headers, json={"max_results": 100}, verify=False, timeout=30,
    ).json()
    exp = next((e for e in search.get("experiments", []) if e["name"] == apply_experiment_name), None)
    if exp is None:
        exp = requests.post(
            f"{mlflow_url}/api/2.0/mlflow/experiments/create",
            headers=headers, json={"name": apply_experiment_name}, verify=False, timeout=30,
        ).json()
        experiment_id = exp["experiment_id"]
    else:
        experiment_id = exp["experiment_id"]

    run_name = f"{batch_id}-{pattern_name}"
    run = requests.post(
        f"{mlflow_url}/api/2.0/mlflow/runs/create",
        headers=headers,
        json={
            "experiment_id": experiment_id, "start_time": int(time.time() * 1000),
            "tags": [{"key": "mlflow.runName", "value": run_name}],
        },
        verify=False, timeout=30,
    ).json()
    run_id = run["run"]["info"]["run_id"]
    artifact_uri = run["run"]["info"]["artifact_uri"]

    metrics_ts = int(time.time() * 1000)
    requests.post(
        f"{mlflow_url}/api/2.0/mlflow/runs/log-batch",
        headers=headers,
        json={
            "run_id": run_id,
            "params": [
                {"key": "batch_id", "value": batch_id},
                {"key": "pattern_name", "value": pattern_name},
                {"key": "autorag_run_id", "value": autorag_run_id},
            ],
            "metrics": [
                {"key": "chunks_created", "value": chunks_created, "timestamp": metrics_ts},
                {"key": "documents_indexed", "value": documents_indexed, "timestamp": metrics_ts},
                # a real mlflow metric now, not just a field buried in the
                # json artifact nobody downstream was reading, so a non
                # zero value actually shows up in the run view
                {"key": "embedding_failures", "value": embedding_failures, "timestamp": metrics_ts},
            ],
        },
        verify=False, timeout=30,
    )

    assert artifact_uri.startswith("s3://"), artifact_uri
    _, path = artifact_uri[len("s3://"):].split("/", 1)
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{minio_endpoint}",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )
    s3.put_object(
        Bucket=artifact_bucket,
        Key=f"{path}/apply_report.json",
        Body=json.dumps(report, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    requests.post(
        f"{mlflow_url}/api/2.0/mlflow/runs/update",
        headers=headers,
        json={"run_id": run_id, "status": "FINISHED", "end_time": int(time.time() * 1000)},
        verify=False, timeout=30,
    )

    print(f"logged mlflow run {run_id} in experiment {apply_experiment_name}")
    return run_id


def _use_minio_creds(task):
    kubernetes.use_secret_as_env(
        task, secret_name="minio-dsp-creds",
        secret_key_to_env={"accesskey": "MINIO_ACCESS_KEY", "secretkey": "MINIO_SECRET_KEY"},
    )


def _use_pgvector_creds(task):
    kubernetes.use_secret_as_env(
        task, secret_name="pgvector-creds",
        secret_key_to_env={"user": "PGVECTOR_USER", "password": "PGVECTOR_PASSWORD"},
    )


@dsl.pipeline(
    name="phase1-apply-pattern-pipeline",
    description="winning autorag pattern -> re chunk + re embed the sanitized batch -> pgvector -> mlflow report",
)
def phase1_apply_pattern_pipeline(
    mlflow_url: str,
    batch_id: str,
    pattern_name: str,
    autorag_run_id: str,
    embedding_model: str,
    chunk_size: int,
    chunk_overlap: int,
    minio_endpoint: str = MINIO_ENDPOINT_DEFAULT,
    raw_bucket: str = RAW_BUCKET_DEFAULT,
    sanitized_prefix: str = SANITIZED_PREFIX_DEFAULT,
    pgvector_host: str = "pgvector.spear-data.svc.cluster.local",
    pgvector_port: str = "5432",
    pgvector_db: str = "ragdb",
    ogx_base_url: str = OGX_BASE_URL_DEFAULT,
    mlflow_workspace: str = MLFLOW_WORKSPACE_DEFAULT,
    ingestion_experiment_name: str = INGESTION_EXPERIMENT_NAME_DEFAULT,
    apply_experiment_name: str = APPLY_EXPERIMENT_NAME_DEFAULT,
    artifact_bucket: str = "dsp-pipeline-artifacts",
):
    apply_task = apply_winning_pattern(
        minio_endpoint=minio_endpoint, bucket=raw_bucket, sanitized_prefix=sanitized_prefix,
        pgvector_host=pgvector_host, pgvector_port=pgvector_port, pgvector_db=pgvector_db,
        ogx_base_url=ogx_base_url, mlflow_url=mlflow_url, mlflow_workspace=mlflow_workspace,
        ingestion_experiment_name=ingestion_experiment_name, batch_id=batch_id,
        pattern_name=pattern_name, autorag_run_id=autorag_run_id,
        embedding_model=embedding_model, chunk_size=chunk_size, chunk_overlap=chunk_overlap,
    )
    _use_minio_creds(apply_task)
    _use_pgvector_creds(apply_task)
    apply_task.set_caching_options(False)

    report_task = log_apply_report(
        mlflow_url=mlflow_url, mlflow_workspace=mlflow_workspace,
        apply_experiment_name=apply_experiment_name, batch_id=batch_id,
        pattern_name=pattern_name, autorag_run_id=autorag_run_id,
        minio_endpoint=minio_endpoint, artifact_bucket=artifact_bucket,
        chunks_created=apply_task.outputs["chunks_created"],
        documents_indexed=apply_task.outputs["documents_indexed"],
        embedding_failures=apply_task.outputs["embedding_failures"],
        apply_report=apply_task.outputs["apply_report"],
    )
    _use_minio_creds(report_task)
    report_task.set_caching_options(False)


if __name__ == "__main__":
    from kfp import compiler

    out = __file__.replace("pipeline.py", "phase1-apply-pattern-pipeline.yaml")
    compiler.Compiler().compile(phase1_apply_pattern_pipeline, out)
    print(f"compiled {out}")
