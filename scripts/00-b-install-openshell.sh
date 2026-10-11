#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
NAMESPACE="${OPENSHELL_NS:-openshell}"
# SANDBOX_NS="${SANDBOX_NS:-agent-pack}"
SANDBOX_NS="${SANDBOX_NS:-spear-shield-agents}"
CHART_VERSION="${OPENSHELL_CHART_VERSION:-0.0.116}"
RELEASE="${OPENSHELL_RELEASE:-openshell}"
# only used below as a last resort if reading the route back fails, see
# 00-a-install-platform.sh for the same detection pattern and why.
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-$(oc get ingresses.config.openshift.io cluster -o jsonpath='{.spec.domain}' 2>/dev/null)}"
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-apps.cluster-crzz2.dyn.redhatworkshops.io}"

log() { printf '== %s ==\n' "$*"; }

main() {
  command -v oc >/dev/null 2>&1 || {
    echo "oc is required" >&2
    exit 1
  }
  command -v helm >/dev/null 2>&1 || {
    echo "helm is required" >&2
    exit 1
  }

  log "openshell namespace"
  oc create namespace "${NAMESPACE}" --dry-run=client -o yaml | oc apply -f -
  oc create namespace "${SANDBOX_NS}" --dry-run=client -o yaml | oc apply -f -

  log "install openshell gateway"
  helm upgrade --install "${RELEASE}" oci://ghcr.io/nvidia/openshell/helm-chart \
    --version "${CHART_VERSION}" \
    --namespace "${NAMESPACE}" \
    --values "${ROOT_DIR}/manifests/platform/openshell/values-openshell-spear.yaml"

  log "wait for openshift gateway"
  oc -n "${NAMESPACE}" rollout status statefulset/"${RELEASE}" --timeout=300s
  sed "s|CLUSTER_DOMAIN_PLACEHOLDER|${CLUSTER_DOMAIN}|g" \
    "${ROOT_DIR}/manifests/platform/openshell/route.yaml" | oc apply -f -

  log "sandbox service account scc in ${SANDBOX_NS}"
  oc adm policy add-scc-to-user privileged -z openshell-sandbox -n "${SANDBOX_NS}" 2>/dev/null || true

  route_host="$(oc -n "${NAMESPACE}" get route "${RELEASE}" -o jsonpath='{.spec.host}' 2>/dev/null || true)"
  if [[ -z "${route_host}" ]]; then
    route_host="openshell-${SANDBOX_NS}.${CLUSTER_DOMAIN}"
  fi

  cat <<EOF

OpenShell gateway is ready.

route: http://${route_host}

next step:
  ./scripts/00-install-spear.sh

EOF
}

main "$@"
