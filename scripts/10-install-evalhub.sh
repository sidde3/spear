#!/usr/bin/env bash
set -euo pipefail
# stands up a real evalhub instance (trustyai.opendatahub.io/v1, bundled
# inside rhods-operator 3.5 already on this cluster) with ragas-v062,
# ibm-clear-v060 and garak registered. the stock ragas/ibm-clear configmaps
# this cluster ships are not registered at all, confirmed live their
# bundled v0.5.0 images have real bugs, see
# manifests/spear-shield-eval/03-custom-providers.yaml, no reason to enable
# a provider nothing here ever submits a job against.
#
# run after 03-install-data.sh (pgvector) and 09-install-worktracker-and-agents.sh
# (mlflow tracing on both agents, the trace data ibm-clear reads straight
# out of mlflow). submitting the actual evaluation jobs is a separate,
# manual step, scripts/11-submit-evalhub-jobs.sh, not run from here.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=./lib-common.sh
source "${ROOT_DIR}/scripts/lib-common.sh"

EVAL_DB_NAME="evalhub"
EVAL_DB_ROLE="evalhub_app"
RAGAS_BUCKET="evalhub-ragas-data"

# a second database on the same already running pgvector postgres server
# (manifests/spear-data/01-pgvector.yaml), not a dedicated postgres
# instance of its own. raguser is that server's real bootstrap superuser
# (POSTGRES_USER on the pgvector deployment itself), same idiom
# 03-install-data.sh's own ensure_rag_reader_role already uses for a new
# role, createdb here needs a real CREATE DATABASE too since a role alone
# is not enough for evalhub's own database to exist.
ensure_evalhub_database() {
  local password
  password="$(oc get secret evalhub-pg-creds -n "${EVAL_NS}" -o jsonpath='{.data.password}' | base64 -d)"
  oc exec deploy/pgvector -n "${DATA_NS}" -- \
    psql -U raguser -d postgres -v ON_ERROR_STOP=1 -c "
      DO \$\$
      BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${EVAL_DB_ROLE}') THEN
          CREATE ROLE ${EVAL_DB_ROLE} LOGIN;
        END IF;
      END
      \$\$;
      ALTER ROLE ${EVAL_DB_ROLE} PASSWORD '${password}';
    "
  if ! oc exec deploy/pgvector -n "${DATA_NS}" -- \
      psql -U raguser -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '${EVAL_DB_NAME}'" | grep -q 1; then
    oc exec deploy/pgvector -n "${DATA_NS}" -- \
      psql -U raguser -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE ${EVAL_DB_NAME} OWNER ${EVAL_DB_ROLE};"
  fi
  log "${EVAL_DB_NAME} database present on pgvector, owned by ${EVAL_DB_ROLE}"
}

# evalhub's own job pods download ragas test data from s3, not a
# projected volume or inline body, the secret shape it expects
# (AWS_ACCESS_KEY_ID etc) is different from the accesskey/secretkey
# shape minio-root-creds/minio-dsp-creds already use elsewhere in this
# project, so this is a re keyed copy of the same real minio root
# credentials, not a new minio user.
ensure_ragas_bucket_and_s3_secret() {
  local user pass
  user=$(oc get secret minio-root-creds -n "${STORAGE_NS}" -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)
  pass=$(oc get secret minio-root-creds -n "${STORAGE_NS}" -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)

  ensure_secret evalhub-s3-credentials "${EVAL_NS}" \
    --from-literal=AWS_ACCESS_KEY_ID="${user}" \
    --from-literal=AWS_SECRET_ACCESS_KEY="${pass}" \
    --from-literal=AWS_DEFAULT_REGION=us-east-1 \
    --from-literal=AWS_S3_ENDPOINT="http://minio.${STORAGE_NS}.svc.cluster.local:9000"

  log "creating the ${RAGAS_BUCKET} bucket if it does not exist yet"
  oc delete job evalhub-bucket-bootstrap -n "${STORAGE_NS}" --ignore-not-found=true --wait=true
  cat <<EOF | oc apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: evalhub-bucket-bootstrap
  namespace: ${STORAGE_NS}
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: mc
          image: quay.io/eformat/mc:RELEASE.2025-08-13T08-35-41Z
          env:
            - name: MC_CONFIG_DIR
              value: /tmp/.mc
          envFrom:
            - secretRef:
                name: minio-root-creds
          command:
            - sh
            - -c
            - |
              set -eu
              mc alias set local http://minio.${STORAGE_NS}.svc.cluster.local:9000 "\${MINIO_ROOT_USER}" "\${MINIO_ROOT_PASSWORD}"
              mc mb --ignore-existing local/${RAGAS_BUCKET}
EOF
  oc wait --for=condition=Complete job/evalhub-bucket-bootstrap -n "${STORAGE_NS}" --timeout=120s
}

# builds spear-confidential-data-provider from evalhub/ and registers its
# own provider configmap with the digest actually just pushed, not the
# stale one 05-confidential-data-provider-config.yaml's own yaml hardcodes.
# confirmed live, this project's first real attempt to stand up evalhub:
# neither 04-confidential-data-provider-build.yaml nor
# 05-confidential-data-provider-config.yaml was ever applied by this
# script at all, despite both existing and 01-evalhub.yaml's own cr
# already listing spear-confidential-data as one of its four providers,
# so the operator's own reconcile failed outright: "Failed to reconcile
# Provider ConfigMaps: provider \"spear-confidential-data\" not found".
# even applying 05 as committed would not have been enough on its own,
# its pinned image digest is from whatever cluster it was last built on,
# not this one, see that file's own comment on why it is pinned rather
# than :latest, bumping it by hand here the same way
# 08-install-console.sh's own image_digest_ref already does for the
# console images, not something a human has to remember to do manually
ensure_confidential_data_provider() {
  oc apply -f "${ROOT_DIR}/manifests/spear-shield-eval/04-confidential-data-provider-build.yaml"
  log "building spear-confidential-data-provider from evalhub/"
  oc start-build spear-confidential-data-provider --from-dir="${ROOT_DIR}/evalhub" --wait -n "${EVAL_NS}"

  local repo digest image
  repo="$(oc get is spear-confidential-data-provider -n "${EVAL_NS}" -o jsonpath='{.status.dockerImageRepository}')"
  digest="$(oc get is spear-confidential-data-provider -n "${EVAL_NS}" -o jsonpath='{.status.tags[0].items[0].image}')"
  image="${repo}@${digest}"
  log "registering spear-confidential-data provider configmap, image ${image}"
  sed -E "s|image: image-registry\.openshift-image-registry\.svc:5000/spear-shield-eval/spear-confidential-data-provider@sha256:[0-9a-f]+|image: ${image}|" \
    "${ROOT_DIR}/manifests/spear-shield-eval/05-confidential-data-provider-config.yaml" | oc apply -f -
}

main() {
  command -v oc >/dev/null 2>&1 || { echo "oc is required" >&2; exit 1; }
  command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }

  log "namespace, tenant label"
  oc apply -f "${ROOT_DIR}/manifests/spear-shield-eval/00-namespace.yaml"

  log "evalhub's own postgres database, on the already running pgvector server in ${DATA_NS}"
  ensure_secret evalhub-pg-creds "${EVAL_NS}" \
    --from-literal=user="${EVAL_DB_ROLE}" \
    --from-literal=password="$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)"
  ensure_evalhub_database
  ensure_secret evalhub-db-credentials "${EVAL_NS}" \
    --from-literal=db-url="postgres://${EVAL_DB_ROLE}:$(oc get secret evalhub-pg-creds -n "${EVAL_NS}" -o jsonpath='{.data.password}' | base64 -d)@pgvector.${DATA_NS}.svc.cluster.local:5432/${EVAL_DB_NAME}"

  log "s3 bucket and credentials for ragas's test data, on the already running minio in ${STORAGE_NS}"
  ensure_ragas_bucket_and_s3_secret

  # confirmed live: this cluster's bundled v0.5.0 of both ragas and
  # ibm-clear have real bugs (ragas's llm wrapper vs its own pinned
  # library's newer metrics api, ibm-clear's trace fetch against this
  # mlflow server's v3 schema). these two newer named providers point at
  # the same quay.io images on later tags instead, see
  # manifests/spear-shield-eval/03-custom-providers.yaml for why a new
  # name (not editing ragas/ibm-clear in place) is what actually survives
  # the operator's own reconcile loop.
  log "custom provider configmaps: ragas-v062, ibm-clear-v060, on the operator's own newer quay.io tags"
  oc apply -f "${ROOT_DIR}/manifests/spear-shield-eval/03-custom-providers.yaml"

  log "spear-confidential-data provider: build + register, our own, not a community override"
  ensure_confidential_data_provider

  log "the real evalhub cr, providers: ragas-v062, ibm-clear-v060, garak, spear-confidential-data"
  oc apply -f "${ROOT_DIR}/manifests/spear-shield-eval/01-evalhub.yaml"
  # oc wait fails immediately, not by polling, if the resource does not
  # exist yet at all, confirmed live: this cr's own operator needs a
  # moment to reconcile a brand new cr into its first real deployment,
  # "Error from server (NotFound): deployments.apps evalhub not found"
  # on a plain run straight after the apply above, wait for the object
  # to exist first, then for it to actually report available
  for _ in $(seq 1 30); do
    oc get deployment/evalhub -n "${EVAL_NS}" >/dev/null 2>&1 && break
    sleep 5
  done
  oc wait --for=condition=Available deployment/evalhub -n "${EVAL_NS}" --timeout=300s

  log "caller serviceaccount + rbac for submitting jobs"
  oc apply -f "${ROOT_DIR}/manifests/spear-shield-eval/02-caller-rbac.yaml"

  # confirmed live: every evaluation job's adapter pod gets MLFLOW_WORKSPACE
  # forced to whatever namespace the job actually runs in (the x-tenant
  # header's own namespace), not this cr's own spec.env default. ibm-clear's
  # mlflow_traces_experiment_name lookup only ever sees experiments already
  # in that same namespace's own mlflow workspace, and
  # spear-shield-security-trace lives in spear-pipelines, so
  # spear-pipelines has to be a second real evalhub tenant too, not just
  # spear-shield-eval's own, empty workspace.
  log "registering ${PIPELINES_NS} as a second evalhub tenant, where spear-shield-security-trace's own mlflow workspace actually lives"
  oc apply -f "${ROOT_DIR}/manifests/spear-pipelines/08-evalhub-tenant.yaml"

  # ibm-clear's own litellm backend refuses to call any openai compatible
  # endpoint without *some* non empty api key client side, even though the
  # raw ogx server it actually calls needs none, confirmed live. a dummy
  # value is enough.
  log "dummy model auth secret for ibm-clear's litellm backend in ${PIPELINES_NS}"
  oc apply -f "${ROOT_DIR}/manifests/spear-pipelines/09-evalhub-model-auth.yaml"

  log "evalhub ready"
  local route
  route="$(oc get route evalhub -n "${EVAL_NS}" -o jsonpath='{.spec.host}' 2>/dev/null || true)"
  cat <<EOF

evalhub url: https://${route:-pending}
providers: ragas-v062, ibm-clear-v060, garak
next step:
  ./scripts/11-submit-evalhub-jobs.sh
EOF
}

main "$@"
