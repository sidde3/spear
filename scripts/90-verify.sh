#!/usr/bin/env bash
set -uo pipefail
# read only health check across everything scripts 01 through 09 install.
# doesn't set -e on purpose: it keeps checking every component and prints a
# pass/fail summary at the end, rather than stopping at the first failure.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=./lib-common.sh
source "${ROOT_DIR}/scripts/lib-common.sh"

FAILURES=0

check() {
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then
    printf '  [ok]   %s\n' "${desc}"
  else
    printf '  [FAIL] %s\n' "${desc}"
    FAILURES=$((FAILURES + 1))
  fi
}

main() {
  log "namespaces"
  for ns in "${DATA_NS}" "${STORAGE_NS}" "${INFERENCE_NS}" "${GUARDRAILS_NS}" \
            "${PIPELINES_NS}" "${CONSOLE_NS}" "${WORKTRACKER_NS}" "${AGENTS_NS}"; do
    check "${ns} namespace exists" oc get namespace "${ns}"
  done

  log "odh dashboard config"
  check "genAiStudio flag is true" bash -c \
    "[[ \"\$(oc get odhdashboardconfig odh-dashboard-config -n redhat-ods-applications -o jsonpath='{.spec.dashboardConfig.genAiStudio}')\" == 'true' ]]"
  check "autorag flag is true" bash -c \
    "[[ \"\$(oc get odhdashboardconfig odh-dashboard-config -n redhat-ods-applications -o jsonpath='{.spec.dashboardConfig.autorag}')\" == 'true' ]]"

  log "pgvector (${DATA_NS})"
  check "pgvector deployment available" oc wait --for=condition=Available deployment/pgvector -n "${DATA_NS}" --timeout=5s
  check "vector extension enabled on ragdb" oc exec deploy/pgvector -n "${DATA_NS}" -- \
    psql -U raguser -d ragdb -tAc "select 1 from pg_extension where extname='vector'"

  log "minio (${STORAGE_NS})"
  check "minio deployment available" oc wait --for=condition=Available deployment/minio -n "${STORAGE_NS}" --timeout=5s
  check "minio-dsp-creds synced into ${PIPELINES_NS}" oc get secret minio-dsp-creds -n "${PIPELINES_NS}"
  check "seed corpus configmap present" oc get configmap spear-seed-docs -n "${STORAGE_NS}"

  log "pipeline server (${PIPELINES_NS})"
  check "dspa reports Ready" bash -c \
    "[[ \"\$(oc get dspa rag-phase1-dspa -n ${PIPELINES_NS} -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}')\" == 'True' ]]"

  log "guardrails (${GUARDRAILS_NS})"
  # reverted back to .status.phase, see scripts/07-install-guardrails.sh's
  # own wait_guardrails_ready comment, an earlier pass here wrongly
  # believed this field could never reach Ready
  check "nemoguardrails reports Ready" bash -c \
    "[[ \"\$(oc get nemoguardrails rag-phase1-guardrails -n ${GUARDRAILS_NS} -o jsonpath='{.status.phase}')\" == 'Ready' ]]"
  check "guardrails route exists" oc get route rag-phase1-guardrails -n "${GUARDRAILS_NS}"
  # kube-rbac-proxy in front of this route reads its own resourceName
  # scoping from an operator generated configmap whose yaml key
  # ("resourceName") the real proxy binary does not actually recognize
  # (the real field is "name"), so that scoping is silently dropped and
  # a plain unscoped "get services" grant is what the live proxy actually
  # checks, see manifests/spear-guardrails/06-pipeline-rbac.yaml and
  # 07-guard-proxy-rbac.yaml's own comments. this check exercises the
  # pipeline runner sa's real call, not just rbac can-i, confirmed to
  # genuinely fail before that fix (every document gets tagged "error")
  # and pass after it
  check "pipeline runner sa can actually call the guardrails /v1/guardrail/checks endpoint (not just rbac can-i)" bash -c \
    "oc run guardrails-verify-probe --rm -i --restart=Never --image=registry.access.redhat.com/ubi9/ubi-minimal:latest -n ${PIPELINES_NS} \
      --overrides='{\"spec\":{\"serviceAccountName\":\"pipeline-runner-rag-phase1-dspa\"}}' --command -- \
      bash -c \"curl -sfk -X POST https://rag-phase1-guardrails.${GUARDRAILS_NS}.svc.cluster.local/v1/guardrail/checks \
        -H 'Content-Type: application/json' -H \\\"Authorization: Bearer \\\$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\\\" \
        -d '{\\\"model\\\":\\\"test\\\",\\\"messages\\\":[{\\\"role\\\":\\\"user\\\",\\\"content\\\":\\\"Hello\\\"}]}'\" | grep -q '\"status\":\"success\"'"

  log "ogx (${INFERENCE_NS})"
  check "ogxserver reports Ready" bash -c \
    "[[ \"\$(oc get ogxserver rag-phase1-ogx -n ${INFERENCE_NS} -o jsonpath='{.status.phase}')\" == 'Ready' ]]"
  check "ogxserver service responds healthy" oc run ogx-verify-probe --rm -i --restart=Never \
    --image=registry.access.redhat.com/ubi9/ubi-minimal:latest -n "${INFERENCE_NS}" --command -- \
    bash -c "curl -sf http://rag-phase1-ogx-service.${INFERENCE_NS}.svc.cluster.local:8321/v1/health | grep -q '\"OK\"'"
  # every other ogx check in this file runs its probe pod inside
  # ${INFERENCE_NS} itself, always allowed by the cr's own podSelector:
  # {} ingress rule regardless of what cross namespace rules exist, so
  # none of them could ever catch a missing cross namespace
  # NetworkPolicy ingress entry. confirmed live, 2026-10-04: the
  # ingestion pipeline's own generate_eval_dataset step, calling this
  # same service from spear-pipelines, got a silently dropped connection
  # (full curl timeout, not a fast refusal) for exactly this reason,
  # see manifests/spear-inference/01-ogxserver.yaml's own networkpolicy
  # section. this check reproduces the real caller's own namespace
  check "ogxserver reachable from spear-pipelines, not just ${INFERENCE_NS} itself (networkpolicy ingress)" \
    oc run ogx-netpol-verify-probe --rm -i --restart=Never \
    --image=registry.access.redhat.com/ubi9/ubi-minimal:latest -n "${PIPELINES_NS}" --command -- \
    bash -c "curl -sf --max-time 10 http://rag-phase1-ogx-service.${INFERENCE_NS}.svc.cluster.local:8321/v1/health | grep -q '\"OK\"'"
  # the vllm foundation model and the embedding model, wired via the
  # image's own generic env var driven remote::vllm slots, see
  # manifests/spear-inference/01-ogxserver.yaml, only meaningful once
  # scripts/05-install-inference.sh has been run.
  check "ogx lists the vllm foundation model plus the embedding model" oc run ogx-models-verify-probe --rm -i \
    --restart=Never --image=registry.access.redhat.com/ubi9/ubi-minimal:latest -n "${INFERENCE_NS}" --command -- \
    bash -c "out=\$(curl -sf http://rag-phase1-ogx-service.${INFERENCE_NS}.svc.cluster.local:8321/v1/models); \
      echo \"\${out}\" | grep -q vllm-inference/ && echo \"\${out}\" | grep -q nomic-embed"
  # a real failed autorag run on 2026-09-28 (see PHASE1_PLAN.md) proved
  # this is the check that actually matters for run_autorag.py's own
  # GENERATION_MODELS: ai4rag's search space preparation step only ever
  # checks /v1/models presence, it never calls /v1/chat/completions
  # itself, so a model missing from this listing fails every autorag run
  # with a SearchSpaceValueError regardless of whether it would have
  # routed a real completion correctly. check exactly the models
  # run_autorag.py configures, not a stand in.
  #
  # correction, 2026-10-10: this used to check for
  # vllm-inference/Qwen3.6-35B-A3B, see manifests/spear-inference/01-ogxserver.yaml's
  # own comment on the remote::vllm slot. confirmed live this date: the
  # litemaas gateway behind that same VLLM_URL/ogx-litemaas-key now
  # reports itself as gpt-oss-120b instead, an upstream change on
  # litemaas's own side, nothing in this repo's config changed. every
  # other model (embedding, glm-53-flash) was unaffected. updated here,
  # run_autorag.py's own GENERATION_MODELS, and
  # scripts/08-install-console.sh's AUTORAG_GENERATION_MODELS together to
  # adopt gpt-oss-120b as the second autorag generation model, not just
  # patch this check to stop failing
  check "ogx registers both autorag generation models" oc run ogx-models-registered-verify-probe --rm -i \
    --restart=Never --image=registry.access.redhat.com/ubi9/ubi-minimal:latest -n "${INFERENCE_NS}" --command -- \
    bash -c "out=\$(curl -sf http://rag-phase1-ogx-service.${INFERENCE_NS}.svc.cluster.local:8321/v1/models); \
      echo \"\${out}\" | grep -q 'vllm-inference/gpt-oss-120b' && \
      echo \"\${out}\" | grep -q 'openai/publishers/prelude-maas/models/glm-53-flash'"
  # glm-53-flash genuinely does still route a real completion, confirmed
  # both on 2026-09-27 and again while diagnosing the failure above. it is
  # also still present and ready on the upstream maas gateway's own
  # /v1/models right now, confirmed live 2026-09-28, so it was never truly
  # deregistered anywhere: ogx's own /v1/models is just missing it because
  # correction, 2026-10-04: this comment and the check title below used to
  # say ogx's own /v1/models was missing glm-53-flash, caused by ogx only
  # discovering the upstream's model list once at pod startup and that
  # pod's last boot getting an incomplete response (9 of 10 models), see
  # PHASE1_PLAN.md. confirmed live now, after a pod restart somewhere
  # along the way, glm-53-flash is present and listed normally, same as
  # every other model. kept this as its own check regardless: a real
  # chat completion is still a stronger, different signal than just
  # appearing in the listing, so a future listing only regression here
  # still would not get confused with an autorag capable model actually
  # going down, they remain two different things worth checking separately
  check "ogx routes a real chat completion through openai/glm-53-flash" \
    oc run ogx-chat-verify-probe --rm -i \
    --restart=Never --image=registry.access.redhat.com/ubi9/ubi-minimal:latest -n "${INFERENCE_NS}" --command -- \
    bash -c "curl -sf -X POST http://rag-phase1-ogx-service.${INFERENCE_NS}.svc.cluster.local:8321/v1/chat/completions \
      -H 'Content-Type: application/json' \
      -d '{\"model\":\"openai/publishers/prelude-maas/models/glm-53-flash\",\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}],\"max_tokens\":10}' \
      | grep -q chatcmpl"

  log "mlflow"
  check "mlflow reports Available" bash -c \
    "[[ \"\$(oc get mlflow mlflow -n redhat-ods-applications -o jsonpath='{.status.conditions[?(@.type==\"Available\")].status}')\" == 'True' ]]"
  # runs as the same pipeline-runner sa the ingestion pipeline uses, mlflow
  # does its own SubjectAccessReview per request (see
  # manifests/spear-pipelines/02-mlflow-rbac.yaml), so this doubles as an
  # rbac check too. workspace header value renamed from rag-phase1 to
  # spear-pipelines, see scripts/04-install-pipelines.sh
  check "mlflow api reachable with the spear-pipelines workspace header" oc run mlflow-verify-probe --rm -i \
    --restart=Never --image=registry.access.redhat.com/ubi9/ubi-minimal:latest -n "${PIPELINES_NS}" \
    --overrides="{\"spec\":{\"serviceAccountName\":\"pipeline-runner-rag-phase1-dspa\"}}" --command -- \
    bash -c "curl -sfk -H \"Authorization: Bearer \$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\" \
      -H 'X-MLflow-Workspace: ${PIPELINES_NS}' -X POST -H 'Content-Type: application/json' -d '{\"max_results\":50}' \
      https://mlflow.redhat-ods-applications.svc:8443/mlflow/api/2.0/mlflow/experiments/search | grep -q phase1-ingestion"

  log "phase1-ingestion-pipeline"
  # rag_chunks is deliberately left empty for a batch until a winning
  # autorag pattern is applied to it, see PHASE1_PLAN.md's sequencing
  # writeup, 2026-09-28: ingestion itself no longer writes to this table
  # at all, that only happens once pipelines/phase1_apply_pattern/
  # pipeline.py runs, so a row count check here would be meaningless on a
  # freshly ingested batch that has not had a pattern applied yet.
  check "rag_chunks table exists (created lazily by either pipeline on first write)" \
    oc exec deploy/pgvector -n "${DATA_NS}" -- \
    psql -U raguser -d ragdb -tAc "select 1 from information_schema.tables where table_name='rag_chunks'"
  # written by pipelines/phase1_ingestion/pipeline.py's generate-eval-dataset
  # step, a real sdg hub run against the sanitized corpus, see
  # PHASE1_PLAN.md's "SDG Hub eval dataset generation and real AutoRAG
  # trigger" section for the run that produced this.
  check "sdg hub generated a real eval dataset with at least one qa pair" oc run eval-dataset-verify-probe \
    --rm -i --restart=Never --image=registry.access.redhat.com/ubi9/python-312:latest -n "${PIPELINES_NS}" \
    --overrides="{\"spec\":{\"containers\":[{\"name\":\"eval-dataset-verify-probe\",\"image\":\"registry.access.redhat.com/ubi9/python-312:latest\",\"stdin\":true,\"env\":[{\"name\":\"MINIO_ACCESS_KEY\",\"valueFrom\":{\"secretKeyRef\":{\"name\":\"minio-dsp-creds\",\"key\":\"accesskey\"}}},{\"name\":\"MINIO_SECRET_KEY\",\"valueFrom\":{\"secretKeyRef\":{\"name\":\"minio-dsp-creds\",\"key\":\"secretkey\"}}}]}]}}" \
    --command -- bash -c \
    "pip install --quiet boto3 >/dev/null 2>&1 && python3 -c \"
import boto3, os
s3 = boto3.client('s3', endpoint_url='http://minio.${STORAGE_NS}.svc.cluster.local:9000',
                  aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
                  aws_secret_access_key=os.environ['MINIO_SECRET_KEY'])
body = s3.get_object(Bucket='rag-documents', Key='autorag-eval/seed-batch-001_test_data.json')['Body'].read()
import json
rows = json.loads(body)
assert len(rows) > 0 and 'question' in rows[0] and 'correct_answer_document_ids' in rows[0]
\""

  log "autorag trigger"
  check "autorag-s3-creds and autorag-ogx-creds exist" bash -c \
    "oc get secret autorag-s3-creds -n ${PIPELINES_NS} >/dev/null && oc get secret autorag-ogx-creds -n ${PIPELINES_NS} >/dev/null"
  # confirms the real, rhoai auto registered documents-rag-optimization-
  # pipeline is present on this dspa, the same lookup
  # pipelines/phase1_autorag/run_autorag.py does before triggering a run,
  # using the caller's own oc whoami -t token, no separate credential.
  # the dspa route hostname is resolved live here too, never hardcoded,
  # it is unique per cluster
  local dspa_route
  dspa_route="https://$(oc get route ds-pipeline-rag-phase1-dspa -n "${PIPELINES_NS}" -o jsonpath='{.spec.host}' 2>/dev/null)"
  check "documents-rag-optimization-pipeline is registered on rag-phase1-dspa" bash -c \
    "curl -sk -H \"Authorization: Bearer \$(oc whoami -t)\" \
      '${dspa_route}/apis/v2beta1/pipelines?filter=%7B%22predicates%22%3A%5B%7B%22key%22%3A%22name%22%2C%22operation%22%3A%22EQUALS%22%2C%22stringValue%22%3A%22documents-rag-optimization-pipeline%22%7D%5D%7D' \
      | grep -q documents-rag-optimization-pipeline"

  log "phase1-apply-pattern-pipeline"
  # applies a winning autorag pattern to the production index, see
  # pipelines/phase1_apply_pattern/. registered (compile + upload, no
  # run) by scripts/04-install-pipelines.sh's own
  # register_apply_pattern_pipeline, part of the normal install chain
  # now, triggering a real run still needs real batch/pattern data and
  # stays manual, see that pipeline's own compile_and_run.py
  check "phase1-apply-pattern-pipeline is registered on rag-phase1-dspa" bash -c \
    "curl -sk -H \"Authorization: Bearer \$(oc whoami -t)\" \
      '${dspa_route}/apis/v2beta1/pipelines?filter=%7B%22predicates%22%3A%5B%7B%22key%22%3A%22name%22%2C%22operation%22%3A%22EQUALS%22%2C%22stringValue%22%3A%22phase1-apply-pattern-pipeline%22%7D%5D%7D' \
      | grep -q phase1-apply-pattern-pipeline"

  log "spear-worktracker and spear-shield-agents"
  check "spear-openproject deployment available" oc wait --for=condition=Available deployment/spear-openproject -n "${WORKTRACKER_NS}" --timeout=5s
  check "spear-openproject-mcp reports Ready" oc wait mcpserver spear-openproject-mcp -n "${WORKTRACKER_NS}" --for=condition=Ready --timeout=5s
  check "spear-shield-mcp-guard-proxy deployment available" oc wait --for=condition=Available deployment/spear-shield-mcp-guard-proxy -n "${AGENTS_NS}" --timeout=5s
  check "spear-shield-gateway's allowedRoutes is All, not Same, for the cross namespace worktracker route" bash -c \
    "[[ \"\$(oc get gateway spear-shield-gateway -n ${AGENTS_NS} -o jsonpath='{.spec.listeners[0].allowedRoutes.namespaces.from}')\" == 'All' ]]"
  check "spear-shield-rag-query-relay deployment available (manual follow up, see scripts/09-install-worktracker-and-agents.sh)" \
    oc wait --for=condition=Available deployment/spear-shield-rag-query-relay -n "${DATA_NS}" --timeout=5s

  log "spear-console"
  check "phase1-ingestion-console-backend deployment available" oc wait --for=condition=Available deployment/phase1-ingestion-console-backend -n "${CONSOLE_NS}" --timeout=5s
  # exercises the backend's own real sa token against mlflow, not
  # pipeline-runner's, the earlier version of this check used
  # pipeline-runner's sa and missed a real cross namespace rbac gap:
  # mlflow checks rbac in the namespace matching the X-MLflow-Workspace
  # header, spear-pipelines, not the caller's own home namespace, so this
  # backend's own grant has to live in manifests/spear-pipelines/04-console-backend-mlflow-rbac.yaml,
  # not anywhere in spear-console itself
  check "console backend's own sa can see the phase1-ingestion experiment in mlflow (not just pipeline-runner's)" bash -c \
    "oc exec -n ${CONSOLE_NS} deploy/phase1-ingestion-console-backend -- bash -c \"curl -sfk -H \\\"Authorization: Bearer \\\$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)\\\" -H 'X-MLflow-Workspace: ${PIPELINES_NS}' -X POST -H 'Content-Type: application/json' -d '{\\\"max_results\\\":50}' https://mlflow.redhat-ods-applications.svc:8443/mlflow/api/2.0/mlflow/experiments/search\" | grep -q '\"name\": \"phase1-ingestion\"'"

  echo
  if (( FAILURES == 0 )); then
    log "all checks passed"
  else
    log "${FAILURES} check(s) failed, see [FAIL] lines above"
    exit 1
  fi
}

main "$@"
