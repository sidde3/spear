#!/usr/bin/env bash
set -euo pipefail
# builds and deploys the console: the backend (manifests/spear-console)
# plus the frontend openshift console plugin
# (owner-console/charts/openshift-console-plugin). both images are built
# binary, straight from source, into this namespace's own internal
# registry, same pattern every other custom image in this project already
# uses.
#
# NAMESPACE_MIGRATION_PLAN.md: the component itself is renamed, console
# not owner-console, since it is slated to also host the phase 2 chat tab,
# not just phase 1 ingestion controls. the source directories at the repo
# root (owner-console/owner-console-backend) keep their own names for
# now, that deeper rename is deliberately deferred, see the plan's own
# section 1, this script still builds from those paths
#
# MLFLOW_URL, DSPA_ROUTE and GUARDRAILS_ROUTE are never hardcoded anywhere
# in owner-console-backend/server.py: all three are openshift route
# hostnames, unique per cluster, so this script resolves them live and
# injects them into the backend deployment with `oc set env`, not the
# applied manifest itself, so a later `oc apply -f 01-backend.yaml` never
# wipes them out (they are absent from the manifest's own last applied
# configuration, so strategic merge leaves them alone, confirmed against
# how minio-dsp-creds style secrets are already handled in this project).
#
# run scripts/02-install-storage.sh, 04-install-pipelines.sh,
# 05-install-inference.sh and 07-install-guardrails.sh first, this script
# assumes every route it resolves below already exists.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=./lib-common.sh
source "${ROOT_DIR}/scripts/lib-common.sh"

SHARED_NS="redhat-ods-applications"
BACKEND_BC="phase1-ingestion-console-backend"
FRONTEND_BC="phase1-ingestion-console"
BACKEND_DEPLOY="phase1-ingestion-console-backend"
HELM_RELEASE="phase1-ingestion-console"

build_image() {
  local bc="$1" context_dir="$2"
  if ! oc get bc "${bc}" -n "${CONSOLE_NS}" >/dev/null 2>&1; then
    log "creating buildconfig ${bc}"
    oc new-build --binary --strategy=docker --name="${bc}" -n "${CONSOLE_NS}"
  fi
  log "building and pushing ${bc} from ${context_dir}"
  oc start-build "${bc}" -n "${CONSOLE_NS}" --from-dir="${context_dir}" --follow
}

# returns the exact digest just pushed, never the floating :latest tag:
# a deployment referencing :latest with imagePullPolicy IfNotPresent can
# silently keep serving an old cached image after a fresh push, a real
# bug hit and worked around by hand earlier this project, see
# PHASE1_PLAN.md. pinning to the digest here instead makes every apply
# of this script actually roll the pods it just built.
image_digest_ref() {
  local bc="$1" repo digest
  repo=$(oc get is "${bc}" -n "${CONSOLE_NS}" -o jsonpath='{.status.dockerImageRepository}')
  digest=$(oc get is "${bc}" -n "${CONSOLE_NS}" -o jsonpath='{.status.tags[0].items[0].image}')
  echo "${repo}@${digest}"
}

# balakrishnan.b/siddhartha.de also need real accounts in the "sso"
# realm, the one oauth cluster's own identity provider actually uses, not
# spear-shield-agents (that one is only a backend credential store, see
# manifests/spear-console/02-chat-creds.yaml's own header comment). a
# KeycloakRealmImport cannot add users to a realm it does not own, so this
# has to go straight against the admin api, same shape
# manifests/spear-shield-agents/01-keycloak-realm.yaml's own header
# comment already documents for the spire idp/auth flow. password read
# straight out of 02-chat-creds.yaml's own secret below, not a fourth
# hardcoded copy, so one password per persona works at both layers
# without anyone having to remember to edit three files in lockstep.
# must run after 02-chat-creds.yaml is applied. found missing live
# 2026-10-04 after a rebuild, see NAMESPACE_MIGRATION_PLAN.md.
ensure_sso_realm_demo_users() {
  local kc_host admin_user admin_pass admin_token
  kc_host="$(oc get route keycloak -n keycloak -o jsonpath='{.spec.host}')"
  admin_user="$(oc get secret keycloak-initial-admin -n keycloak -o jsonpath='{.data.username}' | base64 -d)"
  admin_pass="$(oc get secret keycloak-initial-admin -n keycloak -o jsonpath='{.data.password}' | base64 -d)"
  admin_token="$(curl -sk -X POST "https://${kc_host}/realms/master/protocol/openid-connect/token" \
    -d 'grant_type=password' -d 'client_id=admin-cli' \
    --data-urlencode "username=${admin_user}" --data-urlencode "password=${admin_pass}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null)"
  if [[ -z "${admin_token}" ]]; then
    echo "warning: could not get a keycloak admin token, skipping sso realm demo user bootstrap" >&2
    return 0
  fi

  local username first last email existing password
  for spec in "balakrishnan.b:Balakrishnan:B" "siddhartha.de:Siddhartha:De"; do
    username="${spec%%:*}"
    first="$(echo "${spec}" | cut -d: -f2)"
    last="$(echo "${spec}" | cut -d: -f3)"
    email="${username}@spear-shield.demo"
    # go-template, not jsonpath: the key itself contains a literal dot
    # (balakrishnan.b), oc's jsonpath dialect has no working way to quote
    # a dotted map key, {.data.balakrishnan.b} reads it as a nested path
    # and {.data['balakrishnan.b']} silently returns nothing, confirmed
    # live testing both. go-template's index function has no such problem.
    password="$(oc get secret spear-shield-console-creds -n "${CONSOLE_NS}" \
      -o go-template="{{index .data \"${username}\"}}" | base64 -d)"
    if [[ -z "${password}" ]]; then
      echo "warning: no ${username} key in spear-shield-console-creds, skipping that user" >&2
      continue
    fi
    existing="$(curl -sk -H "Authorization: Bearer ${admin_token}" \
      "https://${kc_host}/admin/realms/${AGENTS_NS}/users?username=${username}&exact=true" \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["id"] if d else "")' 2>/dev/null)"
    if [[ -n "${existing}" ]]; then
      log "sso realm already has ${username}, leaving the account alone, only syncing its password"
      curl -sk -o /dev/null -X PUT "https://${kc_host}/admin/realms/${AGENTS_NS}/users/${existing}/reset-password" \
        -H "Authorization: Bearer ${admin_token}" -H "Content-Type: application/json" \
        -d "{\"type\":\"password\",\"value\":\"${password}\",\"temporary\":false}"
      continue
    fi
    log "creating ${username} in the sso realm"
    curl -sk -o /dev/null -X POST "https://${kc_host}/admin/realms/${AGENTS_NS}/users" \
      -H "Authorization: Bearer ${admin_token}" -H "Content-Type: application/json" \
      -d "{\"username\":\"${username}\",\"firstName\":\"${first}\",\"lastName\":\"${last}\",\"email\":\"${email}\",\"emailVerified\":true,\"enabled\":true,\"requiredActions\":[],\"credentials\":[{\"type\":\"password\",\"value\":\"${password}\",\"temporary\":false}]}"
  done
}

# wires openshift's own cluster login up to the same spear-shield-agents
# keycloak realm the console/agents already use. confirmed missing
# entirely on a fresh cluster: oauth cluster's own spec starts as {},
# no identity providers at all, so the only way to log into this
# cluster is kube:admin, there is no way to log in as either seeded
# demo persona (balakrishnan.b, siddhartha.de) at all, which the console
# backend's own persona mapping (server.py's ask_spear_shield,
# persona_password) depends on to decide who is actually asking.
# without this, the console's own "ask spear shield" chat page can
# never work for a real browser user, only for kube:admin, who has no
# persona file on disk and predictably fails with "no
# spear-shield-agents realm password on file for 'kube:admin'".
#
# a new, dedicated keycloak client for this, not reusing spear-coordinator:
# that client's own directAccessGrantsEnabled is for the backend's own
# server side password grant, standardFlowEnabled false, the opposite
# of what an interactive browser redirect login needs. see
# manifests/spear-shield-agents/01-keycloak-realm.yaml's own matching
# client entry, documentation only: KeycloakRealmImport is a one time
# import job (see that file's header comment), an already imported
# realm needs this client created via the admin api instead, same as
# ensure_sso_realm_demo_users above already does for users.
#
# idempotent by checking for the openshift-config secret first, not the
# keycloak client: a client with no matching secret recorded here would
# leave oauth cluster patched with a clientSecret reference pointing at
# nothing, the safer order is create-then-record, never the reverse.
# keycloak's route tls, confirmed live, is a real letsencrypt cert on
# this cluster's own wildcard ingress domain (reencrypt termination,
# the backend-facing leg uses the service-ca issued cert instead, see
# manifests/platform/04-keycloak-server.yaml), so oauth-openshift's own
# default trust store already trusts the issuer url below with no
# custom ca needed, confirmed live with a plain curl, no -k
ensure_openshift_oauth_idp() {
  local idp_name="spear-shield-agents" client_id="openshift-console"
  local kc_host cluster_domain redirect_uri issuer
  kc_host="$(oc get route keycloak -n keycloak -o jsonpath='{.spec.host}')"
  cluster_domain="$(oc get ingresses.config.openshift.io cluster -o jsonpath='{.spec.domain}')"
  redirect_uri="https://oauth-openshift.${cluster_domain}/oauth2callback/${idp_name}"
  issuer="https://${kc_host}/realms/${AGENTS_NS}"

  if oc get secret keycloak-openshift-client-secret -n openshift-config >/dev/null 2>&1; then
    log "openshift-console keycloak client secret already recorded, leaving the client alone"
  else
    local admin_user admin_pass admin_token
    admin_user="$(oc get secret keycloak-initial-admin -n keycloak -o jsonpath='{.data.username}' | base64 -d)"
    admin_pass="$(oc get secret keycloak-initial-admin -n keycloak -o jsonpath='{.data.password}' | base64 -d)"
    # -sS --fail, not -sk: confirmed live this cluster's keycloak route
    # presents a real, publicly trusted let's encrypt cert (see this
    # function's own header comment), so verifying it costs nothing here
    # and actually matters for this one, unlike the rest of this file's
    # established -sk convention: these calls mint and carry a real
    # keycloak admin token plus a client secret, --fail surfaces a real
    # tls or http error on stderr instead of silently degrading into the
    # same empty-token skip path a wrong password would also produce
    admin_token="$(curl -sS --fail -X POST "https://${kc_host}/realms/master/protocol/openid-connect/token" \
      -d 'grant_type=password' -d 'client_id=admin-cli' \
      --data-urlencode "username=${admin_user}" --data-urlencode "password=${admin_pass}" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null)"
    if [[ -z "${admin_token}" ]]; then
      echo "warning: could not get a keycloak admin token, skipping openshift oauth idp setup" >&2
      return 0
    fi

    local client_uuid
    client_uuid="$(curl -sS --fail -H "Authorization: Bearer ${admin_token}" \
      "https://${kc_host}/admin/realms/${AGENTS_NS}/clients?clientId=${client_id}" \
      | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["id"] if d else "")' 2>/dev/null)"
    if [[ -z "${client_uuid}" ]]; then
      log "creating the ${client_id} client in the ${AGENTS_NS} realm, redirect ${redirect_uri}"
      # -S, not --fail: a real creation failure should still fall through
      # to the re-lookup and warn-and-skip below rather than aborting
      # this whole install script via set -e, same soft-skip contract
      # every other guard in this function already uses
      curl -sS -o /dev/null -X POST "https://${kc_host}/admin/realms/${AGENTS_NS}/clients" \
        -H "Authorization: Bearer ${admin_token}" -H "Content-Type: application/json" \
        -d "{\"clientId\":\"${client_id}\",\"name\":\"OpenShift console login\",\"protocol\":\"openid-connect\",\"publicClient\":false,\"standardFlowEnabled\":true,\"directAccessGrantsEnabled\":false,\"serviceAccountsEnabled\":false,\"clientAuthenticatorType\":\"client-secret\",\"redirectUris\":[\"${redirect_uri}\"]}"
      client_uuid="$(curl -sS --fail -H "Authorization: Bearer ${admin_token}" \
        "https://${kc_host}/admin/realms/${AGENTS_NS}/clients?clientId=${client_id}" \
        | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d[0]["id"] if d else "")' 2>/dev/null)"
    fi
    if [[ -z "${client_uuid}" ]]; then
      echo "warning: could not create or find the ${client_id} keycloak client, skipping openshift oauth idp setup" >&2
      return 0
    fi

    local client_secret
    client_secret="$(curl -sS --fail -H "Authorization: Bearer ${admin_token}" \
      "https://${kc_host}/admin/realms/${AGENTS_NS}/clients/${client_uuid}/client-secret" \
      | python3 -c 'import json,sys; print(json.load(sys.stdin).get("value",""))' 2>/dev/null)"
    if [[ -z "${client_secret}" ]]; then
      echo "warning: could not read the ${client_id} client secret, skipping openshift oauth idp setup" >&2
      return 0
    fi

    log "storing the ${client_id} client secret in openshift-config"
    oc create secret generic keycloak-openshift-client-secret -n openshift-config \
      --from-literal=clientSecret="${client_secret}"
  fi

  log "wiring ${idp_name} into oauth cluster as an openid identity provider, if not already present"
  oc get oauth cluster -o json | python3 -c "
import json, sys
obj = json.load(sys.stdin)
spec = obj.setdefault('spec', {})
idps = spec.setdefault('identityProviders', [])
if not any(p.get('name') == '${idp_name}' for p in idps):
    idps.append({
        'name': '${idp_name}',
        'mappingMethod': 'claim',
        'type': 'OpenID',
        'openID': {
            'clientID': '${client_id}',
            'clientSecret': {'name': 'keycloak-openshift-client-secret'},
            'claims': {'preferredUsername': ['preferred_username'], 'email': ['email'], 'name': ['name']},
            'issuer': '${issuer}',
        },
    })
json.dump(obj, sys.stdout)
" | oc apply -f -
}

resolve_routes() {
  log "resolving this cluster's own route hostnames, never hardcoded"
  DSPA_ROUTE="https://$(oc get route ds-pipeline-rag-phase1-dspa -n "${PIPELINES_NS}" -o jsonpath='{.spec.host}')"
  GUARDRAILS_ROUTE="https://$(oc get route rag-phase1-guardrails -n "${GUARDRAILS_NS}" -o jsonpath='{.spec.host}')"
  MLFLOW_URL="$(oc get mlflow mlflow -n "${SHARED_NS}" -o jsonpath='{.status.url}')"
  MINIO_CONSOLE_URL="https://$(oc get route minio-console -n "${STORAGE_NS}" -o jsonpath='{.spec.host}')"
  RHOAI_DASHBOARD_URL="https://$(oc get route rhods-dashboard -n "${SHARED_NS}" -o jsonpath='{.spec.host}')"
  # spear shield chat, section 8: same sso host the agents realm already
  # lives on, and the coordinator's own inbound a2a route, see
  # manifests/spear-shield-agents/07-a2a-gateway.yaml
  SPEAR_SHIELD_ISSUER_URL="https://$(oc get route keycloak -n keycloak -o jsonpath='{.spec.host}')/realms/${AGENTS_NS}"
  SPEAR_COORDINATOR_A2A_URL="http://spear-coordinator-a2a-gateway.${AGENTS_NS}.svc.cluster.local:80"
  # same host the mcp gateway's own real listener matches on, same
  # computation 09-install-worktracker-and-agents.sh's own resolve_hosts
  # already does for MCP_PUBLIC_HOST, needed here too for the security
  # demo page's direct tools/call against the work tracker
  local cluster_domain
  cluster_domain="$(oc get ingresses.config.openshift.io cluster -o jsonpath='{.spec.domain}')"
  SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER="mcp-${AGENTS_NS}.${cluster_domain}"
  if [[ -z "${DSPA_ROUTE}" || "${DSPA_ROUTE}" == "https://" ]]; then
    echo "could not resolve the dspa route, run scripts/04-install-pipelines.sh first" >&2
    exit 1
  fi
  if [[ -z "${GUARDRAILS_ROUTE}" || "${GUARDRAILS_ROUTE}" == "https://" ]]; then
    echo "could not resolve the guardrails route, run scripts/07-install-guardrails.sh first" >&2
    exit 1
  fi
  if [[ -z "${MLFLOW_URL}" ]]; then
    echo "could not resolve mlflow's url, run scripts/04-install-pipelines.sh first" >&2
    exit 1
  fi
  if [[ -z "${MINIO_CONSOLE_URL}" || "${MINIO_CONSOLE_URL}" == "https://" ]]; then
    echo "could not resolve the minio console route, run scripts/02-install-storage.sh first" >&2
    exit 1
  fi
  if [[ -z "${RHOAI_DASHBOARD_URL}" || "${RHOAI_DASHBOARD_URL}" == "https://" ]]; then
    echo "could not resolve the rhods-dashboard route, is this a rhoai cluster?" >&2
    exit 1
  fi
  log "dspa route:         ${DSPA_ROUTE}"
  log "guardrails route:   ${GUARDRAILS_ROUTE}"
  log "mlflow url:         ${MLFLOW_URL}"
  log "minio console:      ${MINIO_CONSOLE_URL}"
  log "rhoai dashboard:    ${RHOAI_DASHBOARD_URL}"
  log "spear shield issuer:${SPEAR_SHIELD_ISSUER_URL}"
  log "coordinator a2a url:${SPEAR_COORDINATOR_A2A_URL}"
  log "mcp gateway host:   ${SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER}"
}

main() {
  command -v oc >/dev/null 2>&1 || { echo "oc is required" >&2; exit 1; }
  command -v helm >/dev/null 2>&1 || { echo "helm is required" >&2; exit 1; }

  if ! oc get secret minio-dsp-creds -n "${PIPELINES_NS}" >/dev/null 2>&1; then
    echo "minio-dsp-creds not found in ${PIPELINES_NS}, run ./scripts/02-install-storage.sh first" >&2
    exit 1
  fi
  # 01-backend.yaml's own secretKeyRef only resolves within spear-console,
  # the pod's own namespace, same cross namespace secret limit as
  # pgvector-creds/spear-inference, found live during the same rebuild,
  # see NAMESPACE_MIGRATION_PLAN.md and lib-common.sh's own mirror_secret
  log "mirroring minio-dsp-creds into ${CONSOLE_NS}"
  mirror_secret minio-dsp-creds "${PIPELINES_NS}" "${CONSOLE_NS}"

  build_image "${BACKEND_BC}" "${ROOT_DIR}/owner-console-backend"

  log "applying backend rbac, chat creds and deployment"
  oc apply -f "${ROOT_DIR}/manifests/spear-console/00-backend-rbac.yaml"
  oc apply -f "${ROOT_DIR}/manifests/spear-console/02-chat-creds.yaml"
  oc apply -f "${ROOT_DIR}/manifests/spear-console/03-backend-node-reader.yaml"
  ensure_sso_realm_demo_users
  ensure_openshift_oauth_idp
  oc apply -f "${ROOT_DIR}/manifests/spear-shield-agents/08-console-backend-rbac.yaml"
  oc apply -f "${ROOT_DIR}/manifests/spear-console/01-backend.yaml"

  local backend_image
  backend_image="$(image_digest_ref "${BACKEND_BC}")"
  log "pinning ${BACKEND_DEPLOY} to the digest just pushed: ${backend_image}"
  oc set image "deployment/${BACKEND_DEPLOY}" -n "${CONSOLE_NS}" "backend=${backend_image}"

  resolve_routes
  log "injecting the resolved routes into ${BACKEND_DEPLOY}, safe to rerun"
  oc set env "deployment/${BACKEND_DEPLOY}" -n "${CONSOLE_NS}" \
    "MLFLOW_URL=${MLFLOW_URL}" \
    "DSPA_ROUTE=${DSPA_ROUTE}" \
    "GUARDRAILS_ROUTE=${GUARDRAILS_ROUTE}" \
    "MINIO_CONSOLE_URL=${MINIO_CONSOLE_URL}" \
    "RHOAI_DASHBOARD_URL=${RHOAI_DASHBOARD_URL}" \
    "SPEAR_SHIELD_ISSUER_URL=${SPEAR_SHIELD_ISSUER_URL}" \
    "SPEAR_COORDINATOR_A2A_URL=${SPEAR_COORDINATOR_A2A_URL}" \
    "SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER=${SPEAR_SHIELD_MCP_GATEWAY_HOST_HEADER}"

  # everything below used to be a python constant baked into server.py, see
  # its own module level defaults for the values this reproduces. setting
  # them explicitly here (rather than leaving it to those defaults) means
  # this script stays the one place an operator looks to retune a live
  # deployment, e.g. swapping AUTORAG_GENERATION_MODELS to bring
  # glm-53-flash back once it is confirmed live again in ogx's own
  # /v1/models, without touching server.py or rebuilding the image.
  #
  # NAMESPACE_MIGRATION_PLAN.md: RAG_WORKSPACE changed from rag-phase1 to
  # spear-pipelines, the mlflow workspace label is now carried by that
  # namespace, see scripts/04-install-pipelines.sh. server.py itself still
  # needs a read through to confirm every one of these env vars is the
  # only place it ever assumes a namespace, not rediscovered from a
  # hardcoded default of its own, flagged as a pending application code
  # sweep item, not resolved here
  log "injecting pipeline/experiment names and autorag knobs into ${BACKEND_DEPLOY}, safe to rerun"
  oc set env "deployment/${BACKEND_DEPLOY}" -n "${CONSOLE_NS}" \
    "RAG_WORKSPACE=${PIPELINES_NS}" \
    "INGESTION_EXPERIMENT_NAME=phase1-ingestion" \
    "INGESTION_PIPELINE_NAME=phase1-ingestion-pipeline" \
    "INGESTION_KFP_EXPERIMENT_NAME=phase1-ingestion" \
    "AUTORAG_PIPELINE_NAME=documents-rag-optimization-pipeline" \
    "AUTORAG_KFP_EXPERIMENT_NAME=phase1-autorag" \
    "APPLY_PATTERN_PIPELINE_NAME=phase1-apply-pattern-pipeline" \
    "APPLY_PATTERN_KFP_EXPERIMENT_NAME=phase1-apply-pattern" \
    "AUTORAG_RAW_BUCKET=rag-documents" \
    "AUTORAG_S3_SECRET_NAME=autorag-s3-creds" \
    "AUTORAG_OGX_SECRET_NAME=autorag-ogx-creds" \
    "AUTORAG_VECTOR_IO_PROVIDER_ID=pgvector" \
    "AUTORAG_EMBEDDING_MODELS=vllm-embedding/nomic-embed-text-v1.5" \
    "AUTORAG_GENERATION_MODELS=vllm-inference/gpt-oss-120b,openai/publishers/prelude-maas/models/glm-53-flash" \
    "AUTORAG_MAX_RAG_PATTERNS=8" \
    "AUTORAG_PRESET=balanced" \
    "AUTORAG_OPTIMIZATION_METRIC=faithfulness" \
    "MINIO_ENDPOINT=http://minio.${STORAGE_NS}.svc.cluster.local:9000" \
    "ARTIFACT_BUCKET=dsp-pipeline-artifacts"
  wait_deploy_ready "${BACKEND_DEPLOY}" "${CONSOLE_NS}"

  log "building the frontend plugin bundle"
  ( cd "${ROOT_DIR}/owner-console" && yarn build )
  build_image "${FRONTEND_BC}" "${ROOT_DIR}/owner-console"

  local frontend_image
  frontend_image="$(image_digest_ref "${FRONTEND_BC}")"
  log "deploying the console plugin chart with helm, image ${frontend_image}"
  helm upgrade --install "${HELM_RELEASE}" "${ROOT_DIR}/owner-console/charts/openshift-console-plugin" \
    -n "${CONSOLE_NS}" \
    --set "plugin.image=${frontend_image}"

  cat <<EOF

console deployed. resolved for this cluster, nothing hardcoded:
  mlflow url:       ${MLFLOW_URL}
  dspa route:       ${DSPA_ROUTE}
  guardrails route: ${GUARDRAILS_ROUTE}
  minio console:    ${MINIO_CONSOLE_URL}
  rhoai dashboard:  ${RHOAI_DASHBOARD_URL}
check the console navigation under SpearShield > Knowledge Base once the
console operator finishes rolling the plugin in (watch with
oc get consoleplugin ${HELM_RELEASE}).
EOF
}

main "$@"
