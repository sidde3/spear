#!/usr/bin/env bash
set -euo pipefail

# installs every cluster level prerequisite this project needs: the base rhoai
# platform itself (operator, dsc, dsci, dashboard config), plus authorino,
# rhcl (kuadrant), ossm3/istio, mcp-gateway operator, and the keycloak
# operator plus a real keycloak server instance. run this once per cluster
# before 00-b-install-openshell.sh. every step here is safe to re-run,
# subscriptions and crs are check first or plain idempotent apply, the
# keycloak db password is only ever generated once and never touched again
# on a rerun. the rhoai dsc/dsci/dashboard config are the one exception,
# see ensure_rhoai_platform below for why those are never blindly reapplied.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# read the live cluster's own apps domain rather than hardcoding one, an
# explicit CLUSTER_DOMAIN env var still wins if set. the old hardcoded
# apps.cluster-crzz2... string only survives as a last resort fallback for
# whatever unlikely case runs this before oc is logged in to a cluster.
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-$(oc get ingresses.config.openshift.io cluster -o jsonpath='{.spec.domain}' 2>/dev/null)}"
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-apps.cluster-crzz2.dyn.redhatworkshops.io}"
KEYCLOAK_HOSTNAME="${KEYCLOAK_HOSTNAME:-sso.${CLUSTER_DOMAIN}}"
# kata puts every openshell sandbox pod through a MachineConfig change, on
# a single node cluster that means the one and only node reboots. default
# on since that is what "sandbox pods run in kata" actually requires, set
# to false to install everything else and skip that reboot.
INSTALL_KATA="${INSTALL_KATA:-true}"
# spire/spiffe workload identity for a2a, additive to the keycloak
# client-credentials auth already in place, does not require a reboot.
INSTALL_ZTWIM="${INSTALL_ZTWIM:-true}"

# shellcheck source=./lib-olm.sh
source "${ROOT_DIR}/scripts/lib-olm.sh"

log() { printf '== %s ==\n' "$*"; }

wait_istio_ready() {
  oc wait --for=jsonpath='{.status.state}'=Healthy istio/default --timeout=300s
  oc wait --for=jsonpath='{.status.state}'=Healthy istiocni/default --timeout=300s
}

# olm allows exactly one OperatorGroup per namespace, a second one leaves
# every csv in that namespace stuck in TooManyOperatorGroups forever, on
# any future subscription/csv change, not just the next apply. any cluster
# that already had keycloak or rhoai installed before this project showed
# up already carries its own OperatorGroup in those namespaces, so the
# ones this project's own manifests declare are
# only needed on a genuinely fresh cluster. called right after applying a
# manifest that might have just created a second one: if exactly one
# OperatorGroup remains, it's either a fresh cluster (ours, keep it) or
# already deduped, nothing to do either way. more than one always means
# ours collided with a pre-existing one, so ours, and only ours, comes
# back out.
dedupe_operatorgroup() {
  local ns="$1" ours="$2" count
  count=$(oc get operatorgroup -n "${ns}" --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [[ "${count}" -gt 1 ]]; then
    log "namespace ${ns} already had its own OperatorGroup, removing the one this project created (${ours})"
    oc delete operatorgroup "${ours}" -n "${ns}" --ignore-not-found=true
  fi
}

wait_authorino_ready() {
  local tries=60
  while (( tries > 0 )); do
    if oc get deployment authorino -n openshift-operators >/dev/null 2>&1 &&
       oc wait --for=condition=Available deployment/authorino -n openshift-operators --timeout=30s >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
    tries=$((tries - 1))
  done
  echo "authorino deployment did not become ready" >&2
  return 1
}

# spire server and the spiffe csi driver reach Ready independent of any
# live workload asking for an svid, those two are hard requirements. the
# spire agent's k8s workload attestor needs the node's own hostname to be
# resolvable in cluster dns, true on a standard ipi cluster, not true on
# every bare metal/dev cluster, and the oidc discovery provider needs a
# real svid issued to itself to fetch its own jwks, which depends on that
# same attestation succeeding. both degrade to a warning instead of
# failing the whole install: everything upstream of them is still real
# and working, see ARCHITECTURE.md "SPIFFE workload identity".
wait_ztwim_ready() {
  oc wait --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True \
    spireserver/cluster spiffecsidriver/cluster --timeout=180s
  # confirmed live on a fresh multi node install: the daemonset alone took
  # 127s from creation to its Ready condition flipping true, pulling images
  # across every node plus scc/cert setup, a plain rollout, nothing to do
  # with node attestation. 60s here was tighter than that and printed this
  # warning on a run that was actually completely healthy, matching the
  # 180s the two checks above already get avoids that false positive.
  if ! oc wait --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True \
      spireagent/cluster --timeout=180s >/dev/null 2>&1; then
    echo "warning: spire agent daemonset is not Ready yet." >&2
    echo "warning: usually means this node's hostname does not resolve in cluster dns," >&2
    echo "warning: the k8s workload attestor calls the kubelet by hostname, see ARCHITECTURE.md \"SPIFFE workload identity\"." >&2
  fi
  if ! oc wait --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True \
      spireoidcdiscoveryprovider/cluster --timeout=180s >/dev/null 2>&1; then
    echo "warning: spire oidc discovery provider is not Ready yet." >&2
    echo "warning: it needs its own svid to fetch its jwks, same node attestation dependency as the agent above." >&2
  fi
}

# which machineconfigpool actually carries the nodes agent sandbox pods can
# land on. worker if this cluster has real, schedulable worker nodes,
# master otherwise, the single node cluster case where the one node's
# machineconfiguration.openshift.io/role is master and the worker mcp has
# zero machines. confirmed live: pointing kata at a pool with zero
# machines does not error, it just sits at kataNodes.nodeCount: 0 forever,
# no reboot, nothing, silently stuck rather than failing loudly.
detect_kata_pool() {
  # count nodes that are worker but not master/control-plane. do not use
  # mcp worker.machineCount: once nodes get the kata-oc label they leave
  # the worker mcp for the kata-oc mcp, so machineCount drops to 0 and a
  # re-run would wrongly flip onto master.
  local node roles pure=0
  for node in $(oc get nodes -l 'node-role.kubernetes.io/worker' -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    roles=$(oc get node "${node}" -o go-template='{{range $k,$v := .metadata.labels}}{{$k}}{{"\n"}}{{end}}')
    if grep -qx 'node-role.kubernetes.io/master' <<<"${roles}" ||
       grep -qx 'node-role.kubernetes.io/control-plane' <<<"${roles}"; then
      continue
    fi
    pure=$((pure + 1))
  done
  if (( pure > 0 )); then
    echo worker
  else
    echo master
  fi
}

# a re-run of this script should never touch a KataConfig that is already
# fully installed and healthy, confirmed live this can go wrong in a way
# that is not just wasted work: re-detecting and re-applying against an
# already-healthy install flipped kataConfigPoolSelector from no selector
# (the correct, working worker default) to master on a rerun, which
# relabelled all 3 control plane nodes node-role.kubernetes.io/kata-oc,
# the monitor daemonset then scheduled onto them and sat in
# CreateContainerError since those nodes were never actually kata
# installed, and both already-working worker nodes went through a second
# unnecessary reboot cycle recovering from it. exactly why detect_kata_pool
# read master that one time was never pinned down, this guard makes it not
# matter: skip the whole detect/label/apply sequence outright once
# kataNodes.readyNodeCount == kataNodes.nodeCount > 0 and nothing is
# InProgress.
kata_already_healthy() {
  local inprogress ready total
  inprogress=$(oc get kataconfig example-kataconfig -o jsonpath='{.status.conditions[?(@.type=="InProgress")].status}' 2>/dev/null || true)
  ready=$(oc get kataconfig example-kataconfig -o jsonpath='{.status.kataNodes.readyNodeCount}' 2>/dev/null || true)
  total=$(oc get kataconfig example-kataconfig -o jsonpath='{.status.kataNodes.nodeCount}' 2>/dev/null || true)
  [[ "${inprogress}" != "True" && -n "${ready}" && "${ready}" -gt 0 && "${ready}" == "${total}" ]]
}

# the sandboxed-containers-operator's own kata-oc MachineConfigPool only
# ever gains members through this label, it does not add it to nodes
# itself, confirmed live: with no nodes carrying it, the KataConfig
# controller sits at "Waiting for MCO to start updating." forever, no
# error, no timeout, indistinguishable from progress unless you diff
# `oc get nodes -l node-role.kubernetes.io/kata-oc` against the pool you
# expect to be enrolled. this is the same manual step the upstream guide
# this project adapted 07-kata-nftables-patch.yaml from documents, it was
# just never needed on the single node cluster this repo started on since
# every KataConfig test there happened before this gap was found.
#
# label every node in the target pool up front, before the KataConfig CR
# exists, rather than one node at a time after. labelling a lone node
# whose kata-oc pool membership the controller has not seen yet reads as
# unexplained drift to it and it can revert the label mid rollout,
# confirmed live on this same cluster, in a run where the controller's
# own pod happened to be drained off that node partway through.
#
# when the pool is worker, skip nodes that also carry master/control-plane.
# many multi node installs stamp node-role.kubernetes.io/worker on masters
# too; labelling those pulls control planes into kata-oc and leaves them
# stuck in waitingToInstall while only the real workers finish.
label_kata_pool_nodes() {
  local pool="$1"
  local node roles
  for node in $(oc get nodes -l "node-role.kubernetes.io/${pool}" -o jsonpath='{.items[*].metadata.name}'); do
    if [[ "${pool}" == "worker" ]]; then
      roles=$(oc get node "${node}" -o go-template='{{range $k,$v := .metadata.labels}}{{$k}}{{"\n"}}{{end}}')
      if grep -qx 'node-role.kubernetes.io/master' <<<"${roles}" ||
         grep -qx 'node-role.kubernetes.io/control-plane' <<<"${roles}"; then
        continue
      fi
    fi
    oc label node "${node}" node-role.kubernetes.io/kata-oc="" --overwrite
  done
}

# builds and applies the KataConfig CR itself. always select the nodes we
# just labelled with kata-oc. omitting the selector (operator "all workers"
# default) also enrolls control planes that stamp node-role.kubernetes.io/worker
# on multi node openshift installs; those then sit in waitingToInstall with
# no guest initrd, and the nftables patch reports "initrd not found" on them.
apply_kataconfig() {
  local pool="$1"
  cat <<EOF | oc apply -f -
apiVersion: kataconfiguration.openshift.io/v1
kind: KataConfig
metadata:
  name: example-kataconfig
  labels:
    app.kubernetes.io/part-of: rhoai-agent-pack
spec:
  # eligibility checking goes through the Node Feature Discovery operator,
  # which this project does not install. /dev/kvm and amd svm were confirmed
  # by hand (oc debug node, ls /dev/kvm, grep svm /proc/cpuinfo) before
  # writing this manifest, false here just skips the nfd dependency, it
  # does not skip the actual hardware requirement.
  checkNodeEligibility: false
  kataConfigPoolSelector:
    matchLabels:
      node-role.kubernetes.io/kata-oc: ""
EOF
}

# the reboot this triggers on a single node cluster takes several minutes,
# api server goes fully unreachable partway through, this is normal, not a
# hang. wait for the whole labelled pool, not the first ready node: patching
# after ready>=1 raced ahead of RuntimeClass creation and of the second
# worker's guest image, confirmed live.
wait_kata_ready() {
  local tries=120 ready=0 total=0 inprogress="" expected
  expected=$(oc get nodes -l 'node-role.kubernetes.io/kata-oc' --no-headers 2>/dev/null | wc -l | tr -d ' ')
  expected="${expected:-0}"
  log "waiting for kata install on ${expected} labelled node(s), this reboots every node in the pool, expect the api server to drop and come back"
  while (( tries > 0 )); do
    ready=$(oc get kataconfig example-kataconfig -o jsonpath='{.status.kataNodes.readyNodeCount}' 2>/dev/null || echo 0)
    total=$(oc get kataconfig example-kataconfig -o jsonpath='{.status.kataNodes.nodeCount}' 2>/dev/null || echo 0)
    inprogress=$(oc get kataconfig example-kataconfig -o jsonpath='{.status.conditions[?(@.type=="InProgress")].status}' 2>/dev/null || true)
    if oc get runtimeclass kata >/dev/null 2>&1 \
      && [[ "${inprogress}" != "True" ]] \
      && [[ "${ready}" -gt 0 ]] \
      && [[ "${ready}" == "${total}" ]] \
      && [[ "${ready}" -ge "${expected}" ]]; then
      log "kata runtime ready on ${ready}/${total} node(s)"
      return 0
    fi
    sleep 15
    tries=$((tries - 1))
  done
  echo "kataconfig example-kataconfig did not finish installing on all labelled nodes in time (ready=${ready} total=${total} expected=${expected})" >&2
  return 1
}

# the red hat build of agent sandbox replaces an upstream kubernetes-sigs
# controller/crd set that may already be sitting on the cluster if any..
install_agent_sandbox_operator() {
  local ns="agent-sandbox-system" csv="agent-sandbox-operator.v0.9.0"

  if oc get csv "${csv}" -n "${ns}" -o jsonpath='{.status.phase}' 2>/dev/null | grep -qx Succeeded; then
    log "red hat agent sandbox operator already installed"
    return 0
  fi

  log "remove kubernetes-sigs agent sandbox controller if present"
  oc delete deployment agent-sandbox-controller -n "${ns}" --ignore-not-found=true
  oc delete service agent-sandbox-controller -n "${ns}" --ignore-not-found=true
  oc delete serviceaccount agent-sandbox-controller -n "${ns}" --ignore-not-found=true
  oc delete clusterrolebinding agent-sandbox-controller agent-sandbox-controller-extensions --ignore-not-found=true
  oc delete clusterrole agent-sandbox-controller agent-sandbox-controller-extensions --ignore-not-found=true

  log "remove kubernetes-sigs agent sandbox crds if present"
  # --ignore-not-found only covers a named resource that's missing, not a
  # resource *type* that was never registered, which is the normal case on
  # a fresh cluster, deleting instances by kind errors out hard otherwise.
  if oc get crd sandboxes.agents.x-k8s.io >/dev/null 2>&1; then
    oc delete sandbox,sandboxclaim,sandboxtemplate,sandboxwarmpool -A --all --ignore-not-found=true --wait=true
  fi
  oc delete crd \
    sandboxclaims.extensions.agents.x-k8s.io \
    sandboxtemplates.extensions.agents.x-k8s.io \
    sandboxwarmpools.extensions.agents.x-k8s.io \
    sandboxes.agents.x-k8s.io \
    --ignore-not-found=true --wait=true

  log "reset failed operator install plan if needed"
  oc delete installplan -n "${ns}" -l operators.coreos.com/agent-sandbox-operator.agent-sandbox-system --ignore-not-found=true
  oc delete csv "${csv}" -n "${ns}" --ignore-not-found=true
  oc delete subscription agent-sandbox-operator -n "${ns}" --ignore-not-found=true

  log "install red hat build of agent sandbox operator"
  # the delete above removes the subscription itself, not just a stale
  # installplan, on a fresh cluster nothing recreates it afterward without
  # this, reapplying is a no-op for every other subscription in this file,
  # those already succeeded earlier in main.
  oc apply -f "${ROOT_DIR}/manifests/platform/00-operators.yaml"
  approve_and_wait_subscription agent-sandbox-operator "${ns}"

  log "wait for controller deployment"
  oc wait --for=condition=Available deployment \
    -l app.kubernetes.io/name=agent-sandbox-controller \
    -n "${ns}" --timeout=300s 2>/dev/null || \
  oc wait --for=condition=Available deployment \
    -l operators.coreos.com/agent-sandbox-operator.agent-sandbox-system \
    -n "${ns}" --timeout=300s

  log "verify red hat agent sandbox crds"
  oc get crd sandboxes.agents.x-k8s.io sandboxclaims.extensions.agents.x-k8s.io \
    sandboxtemplates.extensions.agents.x-k8s.io sandboxwarmpools.extensions.agents.x-k8s.io >/dev/null
  if ! oc get crd sandboxes.agents.x-k8s.io -o jsonpath='{.spec.versions[*].name}' | grep -q v1beta1; then
    echo "expected sandboxes.agents.x-k8s.io to expose v1beta1" >&2
    return 1
  fi
  local image
  image="$(oc get deploy agent-sandbox-controller -n "${ns}" -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null || true)"
  if [[ "${image}" == registry.k8s.io/agent-sandbox/* ]]; then
    echo "upstream kubernetes-sigs controller image is still active: ${image}" >&2
    return 1
  fi
}

# generates the postgres credentials keycloak's db connection uses, once.
# never regenerates against an already provisioned database, that would
# lock everyone out of a running keycloak instance.
ensure_keycloak_db_secret() {
  if oc get secret keycloak-pgsql-user -n keycloak >/dev/null 2>&1; then
    log "keycloak-pgsql-user secret already exists, leaving it alone"
    return 0
  fi
  log "generating keycloak postgres credentials"
  local password
  password=$(openssl rand -base64 24)
  oc create secret generic keycloak-pgsql-user -n keycloak \
    --from-literal=user=keycloak \
    --from-literal=password="${password}" \
    --from-literal=dbname=keycloak \
    --from-literal=host=keycloak-pgsql \
    --from-literal=port=5432
}

# the rhbk operator creates both "keycloak" and "keycloak-service". service-ca
# issues keycloak-tls against whichever service carries the annotation first;
# the reencrypt route must target that same service or the router returns 503.
# prefer the bare "keycloak" service when it exists (matches CN=keycloak.keycloak.svc
# on a typical install); fall back to keycloak-service otherwise.
ensure_keycloak_serving_cert_annotation() {
  local tries=60 svc=""
  while (( tries > 0 )); do
    if oc get svc keycloak -n keycloak >/dev/null 2>&1; then
      svc=keycloak
    elif oc get svc keycloak-service -n keycloak >/dev/null 2>&1; then
      svc=keycloak-service
    fi
    if [[ -n "${svc}" ]]; then
      oc annotate svc "${svc}" -n keycloak \
        service.beta.openshift.io/serving-cert-secret-name=keycloak-tls --overwrite
      oc patch route keycloak -n keycloak --type merge \
        -p "{\"spec\":{\"to\":{\"name\":\"${svc}\"}}}" >/dev/null
      return 0
    fi
    sleep 5
    tries=$((tries - 1))
  done
  echo "keycloak service never appeared, rhbk operator may not be reconciling the cr" >&2
  return 1
}

# the rhoai operator, DSCInitialization and DataScienceCluster singletons,
# and OdhDashboardConfig are shared cluster wide state that other teams'
# workloads may already depend on, kserve, workbenches, model registry, and
# so on. a fresh cluster has none of these four resources yet, so applying
# the captured manifest verbatim is safe. a cluster that already has rhoai
# installed almost certainly has settings beyond what this project needs, so
# this only checks that the two dashboard flags this project actually depends
# on, agentsCatalog and mcpCatalog, are turned on, and warns rather than
# patches if they are not, leaving that decision to whoever owns the
# cluster's rhoai install.
ensure_rhoai_platform() {
  if oc get datasciencecluster default-dsc >/dev/null 2>&1; then
    log "rhoai already installed, leaving DataScienceCluster/DSCInitialization untouched"
    local agents_catalog mcp_catalog
    agents_catalog=$(oc get odhdashboardconfig odh-dashboard-config -n redhat-ods-applications \
      -o jsonpath='{.spec.dashboardConfig.agentsCatalog}' 2>/dev/null || true)
    mcp_catalog=$(oc get odhdashboardconfig odh-dashboard-config -n redhat-ods-applications \
      -o jsonpath='{.spec.dashboardConfig.mcpCatalog}' 2>/dev/null || true)
    if [[ "${agents_catalog}" != "true" || "${mcp_catalog}" != "true" ]]; then
      echo "warning: odh-dashboard-config has agentsCatalog=${agents_catalog:-unset} mcpCatalog=${mcp_catalog:-unset}," >&2
      echo "warning: the agents/mcp servers this project deploys will not be visible in the dashboard until both are true." >&2
      echo "warning: see manifests/platform/01-rhoai.yaml for the flags this project needs." >&2
    fi
    return 0
  fi
  log "installing rhoai 3.5 (operator, dsc, dsci, dashboard config)"
  # dscinitialization/datasciencecluster/odhdashboardconfig don't exist as
  # crds yet on a fresh cluster, only the operatorgroup and subscription in
  # this same file do, so the first apply below only ever creates those two
  # and errors out on the rest, same pattern as sandboxed-containers below.
  oc apply -f "${ROOT_DIR}/manifests/platform/01-rhoai.yaml" 2>/dev/null || true
  dedupe_operatorgroup redhat-ods-operator rhods-operator
  approve_and_wait_subscription rhods-operator redhat-ods-operator
  # crds exist now that the csv succeeded, this apply is what actually
  # creates dsci/dsc/dashboard config.
  oc apply -f "${ROOT_DIR}/manifests/platform/01-rhoai.yaml"
  oc wait --for=jsonpath='{.status.phase}'=Ready dscinitialization/default-dsci --timeout=300s
  oc wait --for=jsonpath='{.status.phase}'=Ready datasciencecluster/default-dsc --timeout=300s
}

main() {
  command -v oc >/dev/null 2>&1 || { echo "oc is required" >&2; exit 1; }
  command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }

  ensure_rhoai_platform

  log "operator subscriptions: authorino, rhcl, service mesh, mcp-gateway, keycloak"
  oc apply -f "${ROOT_DIR}/manifests/platform/00-operators.yaml"
  dedupe_operatorgroup keycloak keycloak-operatorgroup
  approve_and_wait_subscription authorino-operator-stable-redhat-operators-openshift-marketplace openshift-operators
  approve_and_wait_subscription rhcl-operator-stable-redhat-operators-openshift-marketplace openshift-operators
  approve_and_wait_subscription servicemeshoperator3 openshift-operators
  approve_and_wait_subscription mcp-gateway openshift-operators
  approve_and_wait_subscription rhbk-operator keycloak

  log "red hat build of agent sandbox operator"
  install_agent_sandbox_operator

  log "ossm3 control plane (istio + istio-cni)"
  oc apply -f "${ROOT_DIR}/manifests/platform/02-istio.yaml"
  wait_istio_ready

  log "authorino and kuadrant instances"
  oc apply -f "${ROOT_DIR}/manifests/platform/03-authorino-kuadrant.yaml"
  wait_authorino_ready

  log "keycloak server instance"
  ensure_keycloak_db_secret
  sed "s|KEYCLOAK_HOSTNAME_PLACEHOLDER|${KEYCLOAK_HOSTNAME}|g" \
    "${ROOT_DIR}/manifests/platform/04-keycloak-server.yaml" | oc apply -f -
  ensure_keycloak_serving_cert_annotation
  oc wait --for=condition=Ready keycloak/keycloak -n keycloak --timeout=300s

  if [[ "${INSTALL_ZTWIM}" == "true" ]]; then
    log "zero trust workload identity manager (spire)"
    oc apply -f "${ROOT_DIR}/manifests/platform/05-ztwim.yaml"
    approve_and_wait_subscription openshift-zero-trust-workload-identity-manager zero-trust-workload-identity-manager
    sed "s|SPIRE_OIDC_HOSTNAME_PLACEHOLDER|spire-oidc.${CLUSTER_DOMAIN}|g" \
      "${ROOT_DIR}/manifests/platform/07-spire.yaml" | oc apply -f -
    wait_ztwim_ready
  fi

  if [[ "${INSTALL_KATA}" == "true" ]]; then
    log "openshift sandboxed containers (kata)"
    oc apply -f "${ROOT_DIR}/manifests/platform/06-sandboxed-containers.yaml"
    approve_and_wait_subscription sandboxed-containers-operator openshift-sandboxed-containers-operator
    if kata_already_healthy; then
      log "kata already installed and healthy, not touching it"
    else
      local kata_pool
      kata_pool="$(detect_kata_pool)"
      log "targeting the ${kata_pool} machineconfigpool for kata"
      log "labelling ${kata_pool} nodes node-role.kubernetes.io/kata-oc, kataconfig needs this to enroll them"
      label_kata_pool_nodes "${kata_pool}"
      apply_kataconfig "${kata_pool}"
      wait_kata_ready
    fi
    log "patching kata guest initramfs with the nf_tables modules openshell needs"
    "${ROOT_DIR}/scripts/01-patch-kata-nftables.sh"
  fi

  log "platform install complete"
  cat <<EOF

keycloak: https://${KEYCLOAK_HOSTNAME}/
next step:
  ./scripts/00-b-install-openshell.sh
EOF
}

main "$@"
