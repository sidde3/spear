#!/usr/bin/env bash
set -euo pipefail
# NAMESPACE_MIGRATION_PLAN.md section 8's own fold back decision:
# openproject (spear-worktracker) and the agents/gateways (spear-shield-agents)
# never actually need to install independently of each other in practice,
# one script, same as the old 08-install-spear-shield-agents.sh was before
# this migration split its own manifests across two namespaces.
#
# the realm, the self hosted openproject work tracker, spear-openproject-mcp,
# the gateway/auth policy chain in front of it, the guard proxy, and the a2a
# gateway plus cluster spiffeid the two sandboxed agents depend on. see
# PHASE2_PLAN.md sections 2, 4, 5, 6, 7. the sandboxed agents themselves
# (image builds, sandbox create, policy, provider) are now scripted too,
# see deploy_sandboxed_agents below, adapted from the reference project's
# own manifests/30-openshell/deploy-agents-job.yaml.tmpl in the reference repo (dbs-sow-assessment/
# rhoai-agentic-demo), which already proved the real openshell cli flags
# and ordering live: runtime_class_name is snake_case not the kubernetes
# field's own camelCase, providers_v2_enabled has to be turned on globally
# before provider profile import works, sandboxes have to be deleted
# before a provider attached to one can be deleted, and the coordinator
# always stays on runc, never kata, because its supervisor opens a csi
# spiffe workload api socket that a virtiofs mount inside a kata guest
# cannot proxy (kata-containers/kata-containers#13162, unmerged). this
# used to run those commands from the installer's own machine through a
# local port-forward instead of an in cluster job, on the claim that it
# matched every other oc exec/oc cp step in this script. that claim did
# not actually hold up, openshell cli orchestration is a heavier,
# stateful, multi step operation, not a one off exec, so this now
# matches the reference properly: a real Job, built cli image, see
# manifests/spear-shield-agents/openshell/deploy-agents-job.yaml and
# run_agents_deploy_job below
#
# nothing in here is cluster specific. every hostname (this cluster's own
# apps domain, the sso route, the guardrails route) is either constructed
# from `oc get ingresses.config.openshift.io cluster` or resolved from an
# already live Route, same convention scripts/08-install-console.sh already
# uses, never typed in literally. every manifest under manifests/spear-shield-agents/
# and manifests/spear-worktracker/ that needs one of these carries a
# PLACEHOLDER token instead of a real value, substituted here before
# apply, same pattern the sibling rhoai-agentic-demo project's own
# deploy-agent-pack.sh uses.
#
# admin login, both personas' native accounts, the data-governance
# project, its three work packages, and both personas' api tokens are
# all seeded here too, see seed_openproject_demo_data(). an earlier
# version of this comment claimed personal api tokens need each persona
# to log in themselves, there is no admin api for it. found live during
# this build's schema drift recovery (PHASE2_PLAN.md section 4): that is
# true of openproject's own rest api, but not of rails console, which an
# admin already has full access to via oc exec, so this is scripted now
# too, the whole openproject side of this namespace is recoverable from
# a wiped database with a single rerun of this script.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=./lib-common.sh
source "${ROOT_DIR}/scripts/lib-common.sh"

WORKTRACKER_MANIFESTS="${ROOT_DIR}/manifests/spear-worktracker"
AGENTS_MANIFESTS="${ROOT_DIR}/manifests/spear-shield-agents"
# where the real Mlflow cr lives, same constant scripts/08-install-console.sh
# and scripts/04-install-pipelines.sh already use, not re-derived some
# other way here
SHARED_NS="redhat-ods-applications"

# openproject requires lower, upper, numeric, and a special character.
# a fixed prefix plus random alnum plus fixed suffix guarantees all four
# without a lookup table, same spirit as the openssl one liners already
# used below for the other generated secrets in this script.
gen_openproject_password() {
  echo "Sp$(openssl rand -base64 18 | tr -dc 'a-zA-Z0-9' | head -c 16)#7"
}

# the "spire" spiffe identity provider and the "clients-federated"
# authentication flow, neither representable in a KeycloakRealmImport
# spec at all, see 01-keycloak-realm.yaml's own header comment for the
# full why. both are create or update, safe to run on every install,
# live admin api calls against the already imported realm, adapted
# from the reference project's own sync_spiffe_federated_auth and
# enable_federated_jwt_client_auth (rhoai-agentic-demo/scripts/20-install-security.sh).
# without the idp, spear-coordinator-workload's federated-jwt
# token_grant 400s client_not_found, without the auth flow bound it
# 401s invalid_client instead, confirmed live both ways. this used to
# be a live admin api only fix with no accompanying automation, found
# missing again after the 2026-10-04 rebuild, same gap as
# ensure_sso_realm_demo_users below originally had, see
# NAMESPACE_MIGRATION_PLAN.md
ensure_spiffe_federated_auth() {
  local kc_host admin_user admin_pass admin_token kc spire_oidc_host bundle_endpoint
  kc_host="$(oc get route keycloak -n keycloak -o jsonpath='{.spec.host}')"
  admin_user="$(oc get secret keycloak-initial-admin -n keycloak -o jsonpath='{.data.username}' | base64 -d)"
  admin_pass="$(oc get secret keycloak-initial-admin -n keycloak -o jsonpath='{.data.password}' | base64 -d)"
  admin_token="$(curl -sk -X POST "https://${kc_host}/realms/master/protocol/openid-connect/token" \
    -d 'grant_type=password' -d 'client_id=admin-cli' \
    --data-urlencode "username=${admin_user}" --data-urlencode "password=${admin_pass}" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("access_token",""))' 2>/dev/null)"
  if [[ -z "${admin_token}" ]]; then
    echo "warning: could not get a keycloak admin token, skipping spiffe federated auth setup" >&2
    return 0
  fi
  kc="https://${kc_host}/admin/realms/${AGENTS_NS}"

  # the realm import job runs async off the oc apply just above this
  # call in main, wait for the realm to actually exist before hitting
  # its own admin sub api, a short, bounded poll, not assumed instant
  local tries=30
  while (( tries > 0 )); do
    curl -sk -o /dev/null -H "Authorization: Bearer ${admin_token}" "${kc}" && break
    sleep 2
    tries=$((tries - 1))
  done
  if (( tries == 0 )); then
    echo "warning: ${AGENTS_NS} realm never became reachable, skipping spiffe federated auth setup" >&2
    return 0
  fi

  spire_oidc_host="$(oc get route spire-oidc-discovery-provider -n zero-trust-workload-identity-manager -o jsonpath='{.spec.host}' 2>/dev/null || true)"
  if [[ -z "${spire_oidc_host}" ]]; then
    echo "warning: could not resolve spire's own oidc discovery route, skipping spiffe federated auth setup" >&2
    return 0
  fi
  bundle_endpoint="https://${spire_oidc_host}/keys"

  # trustDomain is the one real spire server already live on this
  # cluster, a cluster wide singleton the reference project's own
  # install created, immutable once set, confirmed live, never this
  # project's own spiffe://spear-shield.internal PHASE2_PLAN.md
  # originally called for, see 01-keycloak-realm.yaml's own comment on
  # spear-coordinator-workload for the full story
  local idp_body idp_status
  idp_body="{\"alias\":\"spire\",\"providerId\":\"spiffe\",\"enabled\":true,\"config\":{\"trustDomain\":\"spiffe://agent-pack.internal\",\"bundleEndpoint\":\"${bundle_endpoint}\",\"supportsClientAssertions\":\"true\"}}"
  idp_status="$(curl -sk -o /dev/null -w '%{http_code}' "${kc}/identity-provider/instances/spire" -H "Authorization: Bearer ${admin_token}")"
  if [[ "${idp_status}" == "404" ]]; then
    log "creating spiffe identity provider spire on ${AGENTS_NS}"
    curl -sk -o /dev/null -X POST "${kc}/identity-provider/instances" \
      -H "Authorization: Bearer ${admin_token}" -H "Content-Type: application/json" -d "${idp_body}"
  else
    log "spire identity provider already exists on ${AGENTS_NS}, re-syncing its bundle endpoint"
    curl -sk -o /dev/null -X PUT "${kc}/identity-provider/instances/spire" \
      -H "Authorization: Bearer ${admin_token}" -H "Content-Type: application/json" -d "${idp_body}"
  fi

  # the federated-jwt client authenticator is never wired into the
  # realm's own client authentication flow by default, confirmed live,
  # every spiffe token_grant call 400s client_not_found otherwise no
  # matter how correct the idp/client config already is. copy the
  # builtin clients flow once, this keycloak version (26.6+) already
  # ships federated-jwt in it, confirmed live, no explicit execution
  # add needed, older versions might, so still checked for below, then
  # bind the realm to the copy
  local flow_alias="clients-federated" existing
  existing="$(curl -sk "${kc}/authentication/flows/${flow_alias}/executions" -H "Authorization: Bearer ${admin_token}")"
  if ! printf '%s' "${existing}" | grep -q '"federated-jwt"'; then
    log "copying builtin clients flow to ${flow_alias}"
    curl -sk -o /dev/null -X POST "${kc}/authentication/flows/clients/copy" \
      -H "Authorization: Bearer ${admin_token}" -H "Content-Type: application/json" \
      -d "{\"newName\": \"${flow_alias}\"}"
    existing="$(curl -sk "${kc}/authentication/flows/${flow_alias}/executions" -H "Authorization: Bearer ${admin_token}")"
    if ! printf '%s' "${existing}" | grep -q '"federated-jwt"'; then
      log "this keycloak version does not ship federated-jwt in the builtin flow, adding it explicitly"
      curl -sk -o /dev/null -X POST "${kc}/authentication/flows/${flow_alias}/executions/execution" \
        -H "Authorization: Bearer ${admin_token}" -H "Content-Type: application/json" \
        -d '{"provider": "federated-jwt"}'
    fi
  fi
  log "binding ${AGENTS_NS} realm's clientAuthenticationFlow to ${flow_alias}"
  curl -sk -o /dev/null -X PUT "${kc}" \
    -H "Authorization: Bearer ${admin_token}" -H "Content-Type: application/json" \
    -d "{\"clientAuthenticationFlow\": \"${flow_alias}\"}"
}

resolve_hosts() {
  log "resolving this cluster's own hostnames, never hardcoded"
  CLUSTER_DOMAIN="$(oc get ingresses.config.openshift.io cluster -o jsonpath='{.spec.domain}')"
  if [[ -z "${CLUSTER_DOMAIN}" ]]; then
    echo "could not resolve this cluster's apps domain" >&2
    exit 1
  fi
  SSO_HOST="$(oc get route keycloak -n keycloak -o jsonpath='{.spec.host}' 2>/dev/null || true)"
  if [[ -z "${SSO_HOST}" ]]; then
    echo "could not resolve the sso realm's own route (route 'keycloak' in namespace 'keycloak')" >&2
    exit 1
  fi
  GUARDRAILS_HOST="$(oc get route rag-phase1-guardrails -n "${GUARDRAILS_NS}" -o jsonpath='{.spec.host}' 2>/dev/null || true)"
  if [[ -z "${GUARDRAILS_HOST}" ]]; then
    echo "could not resolve the guardrails route, run scripts/07-install-guardrails.sh first" >&2
    exit 1
  fi
  GUARDRAILS_ROUTE="https://${GUARDRAILS_HOST}"
  SPIRE_OIDC_ROUTE="https://$(oc get route spire-oidc-discovery-provider -n zero-trust-workload-identity-manager -o jsonpath='{.spec.host}' 2>/dev/null || true)"
  if [[ -z "${SPIRE_OIDC_ROUTE}" || "${SPIRE_OIDC_ROUTE}" == "https://" ]]; then
    echo "could not resolve spire's own oidc discovery route (route 'spire-oidc-discovery-provider' in namespace 'zero-trust-workload-identity-manager')" >&2
    exit 1
  fi
  # the two cidrs every openshell sandbox network policy in this project
  # allowlists, read from the real cluster config, not typed in, same
  # values confirmed live earlier this session via this exact command
  CLUSTER_NETWORK_CIDR="$(oc get network.config.openshift.io cluster -o jsonpath='{.spec.clusterNetwork[0].cidr}')"
  SERVICE_NETWORK_CIDR="$(oc get network.config.openshift.io cluster -o jsonpath='{.spec.serviceNetwork[0]}')"
  if [[ -z "${CLUSTER_NETWORK_CIDR}" || -z "${SERVICE_NETWORK_CIDR}" ]]; then
    echo "could not resolve this cluster's own network/service cidrs" >&2
    exit 1
  fi

  # these two are ours, not pre-existing, constructed the same way the
  # reference's own deploy-agent-pack.sh builds MCP_PUBLIC_HOST, not
  # resolved after the fact, since the route/gateway hostname has to be
  # known before the Route/Gateway that will serve it is even applied
  OPENPROJECT_HOST="spear-openproject.${CLUSTER_DOMAIN}"
  MCP_PUBLIC_HOST="mcp-${AGENTS_NS}.${CLUSTER_DOMAIN}"
  ISSUER_URL="https://${SSO_HOST}/realms/${AGENTS_NS}"

  # the real in process mlflow tracing in both agents, PHASE3_PLAN.md
  # section 4. same resolution scripts/08-install-console.sh already
  # uses for the console backend, read from the real Mlflow CR's own
  # status field, never hardcoded or re-derived some other way
  MLFLOW_URL="$(oc get mlflow mlflow -n "${SHARED_NS}" -o jsonpath='{.status.url}' 2>/dev/null || true)"
  if [[ -z "${MLFLOW_URL}" ]]; then
    echo "could not resolve the mlflow cr's own route (mlflow 'mlflow' in namespace '${SHARED_NS}'), run scripts/08-install-console.sh first" >&2
    exit 1
  fi
  MLFLOW_HOST="${MLFLOW_URL#https://}"
  MLFLOW_HOST="${MLFLOW_HOST#http://}"
  MLFLOW_HOST="${MLFLOW_HOST%%/*}"

  log "cluster domain:    ${CLUSTER_DOMAIN}"
  log "sso issuer:        ${ISSUER_URL}"
  log "openproject host:  ${OPENPROJECT_HOST}"
  log "mcp public host:   ${MCP_PUBLIC_HOST}"
  log "guardrails route:  ${GUARDRAILS_ROUTE}"
  log "spire oidc route:  ${SPIRE_OIDC_ROUTE}"
  log "mlflow url:        ${MLFLOW_URL}"
}

apply_templated() {
  local file="$1"
  sed \
    -e "s|OPENPROJECT_HOST_PLACEHOLDER|${OPENPROJECT_HOST}|g" \
    -e "s|MCP_PUBLIC_HOST_PLACEHOLDER|${MCP_PUBLIC_HOST}|g" \
    -e "s|ISSUER_URL_PLACEHOLDER|${ISSUER_URL}|g" \
    -e "s|GUARDRAILS_ROUTE_PLACEHOLDER|${GUARDRAILS_ROUTE}|g" \
    -e "s|SPIRE_OIDC_ROUTE_PLACEHOLDER|${SPIRE_OIDC_ROUTE}|g" \
    -e "s|MCP_BACKEND_SECRET_PLACEHOLDER|${MCP_BACKEND_SECRET}|g" \
    "${file}" | oc apply -f -
}

# same placeholder convention as apply_templated above, but for files the
# openshell cli consumes directly from a local path rather than oc apply,
# manifests/spear-shield-agents/openshell/policies/coordinator-policy.yaml
# and openshell/providers/coordinator-a2a-token-grant.yaml. writes the
# substituted copy to a tmp path and prints it, caller passes that path to
# the relevant openshell command. SSO_HOST_PLACEHOLDER is the bare host
# (no scheme), the policy's own network_policies entries need a bare host,
# not a url. this only substitutes, deploy_sandboxed_agents below is what
# actually runs the real openshell sandbox create / provider import /
# policy set sequence against the rendered output
render_openshell_file() {
  local src="$1" dest="$2"
  sed \
    -e "s|SSO_HOST_PLACEHOLDER|${SSO_HOST}|g" \
    -e "s|ISSUER_URL_PLACEHOLDER|${ISSUER_URL}|g" \
    -e "s|GUARDRAILS_HOST_PLACEHOLDER|${GUARDRAILS_HOST}|g" \
    -e "s|MLFLOW_HOST_PLACEHOLDER|${MLFLOW_HOST}|g" \
    -e "s|CLUSTER_NETWORK_CIDR_PLACEHOLDER|${CLUSTER_NETWORK_CIDR}|g" \
    -e "s|SERVICE_NETWORK_CIDR_PLACEHOLDER|${SERVICE_NETWORK_CIDR}|g" \
    "${src}" > "${dest}"
  log "rendered ${src} -> ${dest}"
}

# same known limitation as the reference project's own
# patch_mcp_ext_proc_buffered (see its ARCHITECTURE.md, "Known operator
# limitation: ext_proc body mode"): the installed mcp-gateway-controller
# (v0.7.1 preview) always regenerates its ext_proc EnvoyFilter with
# request_body_mode: STREAMED, which a plain stdlib http backend's
# tools/call can't parse ("Bad request syntax"), no CRD field exists to
# change it. fix found and confirmed live during this build, section 4:
# let the controller run once to reconcile every MCPServerRegistration to
# Ready, pause it, then hand patch both EnvoyFilters (this gateway's own,
# and agent-pack's, since the controller is shared cluster wide and briefly
# reverts agent-pack's back to STREAMED too while it runs) to BUFFERED,
# and leave the controller paused.
#
# NAMESPACE_MIGRATION_PLAN.md: the envoyfilter itself is still keyed to
# the gateway's own namespace (spear-shield-agents, unchanged), but the
# real MCPServerRegistration to wait on now lives in spear-worktracker,
# not spear-shield-agents, since spear-openproject-mcp's own HTTPRoute
# moved there
patch_mcp_ext_proc_buffered() {
  local filter="mcp-ext-proc-${AGENTS_NS}-gateway"
  log "unpausing mcp-gateway-controller just long enough to reconcile registrations"
  oc scale deployment/mcp-gateway-controller -n openshift-operators --replicas=1
  oc rollout status deployment/mcp-gateway-controller -n openshift-operators --timeout=90s

  for ns in "${WORKTRACKER_NS}" agent-pack; do
    for reg in $(oc get mcpserverregistration -n "${ns}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
      oc wait mcpserverregistration "${reg}" -n "${ns}" --for=condition=Ready --timeout=90s 2>&1 \
        || log "warning: ${reg} in ${ns} not Ready yet, tools/call for its tools may 404 until it is"
    done
  done

  log "pausing mcp-gateway-controller again and patching both ext_proc filters to BUFFERED"
  oc scale deployment/mcp-gateway-controller -n openshift-operators --replicas=0
  oc wait --for=delete pod -l app.kubernetes.io/name=mcp-gateway-controller -n openshift-operators --timeout=60s 2>/dev/null || true

  if oc get envoyfilter mcp-ext-proc-agent-pack-gateway -n agent-pack >/dev/null 2>&1; then
    oc patch envoyfilter mcp-ext-proc-agent-pack-gateway -n agent-pack --type=json \
      -p='[{"op":"replace","path":"/spec/configPatches/0/patch/value/typed_config/processing_mode/request_body_mode","value":"BUFFERED"}]'
  fi
  if oc get envoyfilter "${filter}" -n "${AGENTS_NS}" >/dev/null 2>&1; then
    oc patch envoyfilter "${filter}" -n "${AGENTS_NS}" --type=json \
      -p='[{"op":"replace","path":"/spec/configPatches/0/patch/value/typed_config/processing_mode/request_body_mode","value":"BUFFERED"}]'
  else
    log "warning: ${filter} not found yet, rerun this script once the gateway/registration have reconciled"
  fi
}

# recreates everything a wiped openproject database needs beyond the
# admin account itself (that part is handled by OPENPROJECT_SEED_ADMIN_USER_*
# env vars on the deployment): both personas' native accounts, the
# data-governance project with its work_package_tracking module actually
# enabled (a plain Project.new.save! skips that, found live, left
# view_work_packages silently false for every member until fixed), its
# three work packages, and each persona's personal api token. every step
# is guarded by a find_by check so reruns against an already seeded
# instance are a no-op, and an existing api token is never destroyed and
# reissued, only minted the first time.
seed_openproject_demo_data() {
  local pod bala_pw sidde_pw script_file out_file bala_token sidde_token
  pod="$(oc get pod -n "${WORKTRACKER_NS}" -l app=spear-openproject -o jsonpath='{.items[0].metadata.name}')"
  # generated fresh every run, cheap, only ever actually used below if
  # ensure_user decides that persona doesn't exist yet, never read back
  # out of a secret that may not have these keys yet
  bala_pw="$(gen_openproject_password)"
  sidde_pw="$(gen_openproject_password)"

  script_file="$(mktemp)"
  cat >"${script_file}" <<'RUBY'
def ensure_user(login:, firstname:, lastname:, mail:, password:)
  user = User.find_by(login: login)
  return user if user

  user = User.new(login: login, firstname: firstname, lastname: lastname, mail: mail, admin: false, language: "en")
  user.password = password
  user.password_confirmation = password
  user.status = 1
  user.force_password_change = false
  user.first_login = false
  user.save!
  puts "created user #{login}, id=#{user.id}"
  user
end

balakrishnan = ensure_user(login: "balakrishnan.b", firstname: "Balakrishnan", lastname: "B",
                            mail: "balakrishnan.b@spear-shield.demo", password: ENV.fetch("BALA_PW"))
siddhartha = ensure_user(login: "siddhartha.de", firstname: "Siddhartha", lastname: "De",
                          mail: "siddhartha.de@spear-shield.demo", password: ENV.fetch("SIDDE_PW"))

task_type = Type.find(1)
project = Project.find_by(identifier: "data-governance")
if project.nil?
  project = Project.new(
    name: "Data Governance", identifier: "data-governance", workspace_type: "project", public: false,
    description: "spear shield phase 2 demo project. real work packages backing the openproject mcp tool calls in architecture.html."
  )
  project.save!
  puts "created project data-governance, id=#{project.id}"
end
if project.enabled_module_names.exclude?("work_package_tracking")
  project.enabled_module_names += ["work_package_tracking"]
  project.save!
  puts "enabled work_package_tracking on data-governance"
end
ProjectType.find_or_create_by!(project_id: project.id, type_id: task_type.id)

member_role = Role.find_by!(name: "Member")
[balakrishnan, siddhartha].each do |user|
  next if Member.exists?(project_id: project.id, user_id: user.id)

  Member.create!(project_id: project.id, user_id: user.id, roles: [member_role])
  puts "added #{user.login} as Member on data-governance"
end

in_progress = Status.find(7)
closed = Status.find(12)
normal_priority = IssuePriority.find(8)
admin_user = User.find_by!(login: "admin")

def ensure_wp(subject:, project:, type:, status:, priority:, author:, assignee:)
  return if WorkPackage.exists?(subject: subject, project_id: project.id)

  wp = WorkPackage.new(subject: subject, project: project, type: type, status: status,
                        priority: priority, author: author, assigned_to: assignee)
  wp.save!(validate: false)
  puts "created work package '#{subject}', id=#{wp.id}"
end

ensure_wp(subject: "Review Q3 customer data retention policy compliance", project: project, type: task_type,
          status: in_progress, priority: normal_priority, author: admin_user, assignee: balakrishnan)
ensure_wp(subject: "Approve elevated data export access for analytics pipeline", project: project, type: task_type,
          status: in_progress, priority: normal_priority, author: admin_user, assignee: siddhartha)
ensure_wp(subject: "Audit trail for previously approved data governance exception", project: project, type: task_type,
          status: closed, priority: normal_priority, author: admin_user, assignee: siddhartha)

unless Token::API.exists?(user: balakrishnan)
  puts "TOKEN_FOR_balakrishnan.b=#{Token::API.create_and_return_value(balakrishnan)}"
end
unless Token::API.exists?(user: siddhartha)
  puts "TOKEN_FOR_siddhartha.de=#{Token::API.create_and_return_value(siddhartha)}"
end
RUBY

  oc cp "${script_file}" "${WORKTRACKER_NS}/${pod}:/tmp/spear-openproject-seed.rb"
  rm -f "${script_file}"

  out_file="$(mktemp)"
  oc exec -n "${WORKTRACKER_NS}" "${pod}" -- env BALA_PW="${bala_pw}" SIDDE_PW="${sidde_pw}" \
    bundle exec rails runner /tmp/spear-openproject-seed.rb >"${out_file}" 2>&1
  oc exec -n "${WORKTRACKER_NS}" "${pod}" -- rm -f /tmp/spear-openproject-seed.rb

  sed 's/^/  /' "${out_file}"

  bala_token="$(grep -o 'TOKEN_FOR_balakrishnan\.b=.*' "${out_file}" | cut -d= -f2- || true)"
  sidde_token="$(grep -o 'TOKEN_FOR_siddhartha\.de=.*' "${out_file}" | cut -d= -f2- || true)"

  # only persist a persona password if ensure_user actually created that
  # user this run, same create-once-never-touch rule as every other
  # generated secret in this script, an already existing persona's real
  # password (which may differ from this run's freshly generated one if
  # it was ever changed by hand) must never be overwritten
  if grep -q "created user balakrishnan.b," "${out_file}"; then
    log "persisting newly generated balakrishnan.b password to spear-openproject-login-creds"
    oc patch secret spear-openproject-login-creds -n "${WORKTRACKER_NS}" --type=merge \
      -p "{\"stringData\":{\"balakrishnan.b-password\":\"${bala_pw}\"}}"
  fi
  if grep -q "created user siddhartha.de," "${out_file}"; then
    log "persisting newly generated siddhartha.de password to spear-openproject-login-creds"
    oc patch secret spear-openproject-login-creds -n "${WORKTRACKER_NS}" --type=merge \
      -p "{\"stringData\":{\"siddhartha.de-password\":\"${sidde_pw}\"}}"
  fi
  rm -f "${out_file}"

  if [[ -n "${bala_token}" || -n "${sidde_token}" ]]; then
    log "persisting newly minted api token(s) to spear-openproject-api-tokens"
    if oc get secret spear-openproject-api-tokens -n "${WORKTRACKER_NS}" >/dev/null 2>&1; then
      local patch="{\"stringData\":{"
      [[ -n "${bala_token}" ]] && patch+="\"balakrishnan.b\":\"${bala_token}\","
      [[ -n "${sidde_token}" ]] && patch+="\"siddhartha.de\":\"${sidde_token}\","
      patch="${patch%,}}}"
      oc patch secret spear-openproject-api-tokens -n "${WORKTRACKER_NS}" --type=merge -p "${patch}"
    else
      oc create secret generic spear-openproject-api-tokens -n "${WORKTRACKER_NS}" \
        --from-literal=balakrishnan.b="${bala_token}" \
        --from-literal=siddhartha.de="${sidde_token}"
    fi
  fi
}

# mirrors the reference's own detect_agent_runtime_class: kata needs a
# real /dev/kvm underneath, not just the RuntimeClass object existing,
# confirmed live there that cloud instance types without nested virt
# fail at pod sandbox creation with no software fix available. checks
# one real kata-oc node directly rather than assuming the cluster's own
# install notes are still accurate. only the retrieval agent is ever a
# candidate for kata, the coordinator is runc always, see the comment
# above main() for why
detect_agent_runtime_class() {
  local node
  node="$(oc get nodes -l node-role.kubernetes.io/kata-oc -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "${node}" ]]; then
    echo ""
    return
  fi
  if oc debug node/"${node}" -- chroot /host test -e /dev/kvm >/dev/null 2>&1; then
    echo "kata"
  else
    echo ""
  fi
}

# the Sandbox controller's own pod template defaults restartPolicy: Never,
# confirmed live on the v1beta1 CRD: a Never pod on a drained or rebooted
# node is never restarted by the kubelet, the pod object just sits
# ContainerStatusUnknown / Failed while the sandbox's operatingMode still
# says Running, exactly the "sat Failed for a full day" state the
# delete-before-provider comment above already recorded after the last
# cluster restart. patching Always lets the kubelet restart the container
# itself on an in-place node reboot, no manual re-run of this script.
# best_effort on purpose: if the v1beta1 schema ever rejects the field,
# a warning here beats a failed install over a non critical patch.
patch_sandbox_restart_policy() {
  local sandbox="$1"
  if oc patch sandbox "${sandbox}" -n "${AGENTS_NS}" \
    --type=merge \
    -p '{"spec":{"podTemplate":{"spec":{"restartPolicy":"Always"}}}}' 2>&1; then
    log "restartPolicy=Always patched onto sandbox ${sandbox}, kubelet now self-heals node reboots"
  else
    echo "warning: could not patch restartPolicy on sandbox ${sandbox}, node reboots will still need a manual re-run" >&2
  fi
}

# jobs are immutable once created, oc apply on an existing one just
# errors on the first changed field, so a rerun always deletes first.
# --wait blocks until the old pod is actually gone, otherwise the new
# job's own pod could start before the old one finished terminating
delete_job_if_exists() {
  local name="$1" ns="$2"
  if oc get "job/${name}" -n "${ns}" >/dev/null 2>&1; then
    log "deleting existing job ${name} in ${ns} before recreating it"
    oc delete "job/${name}" -n "${ns}" --wait=true --timeout=60s
  fi
}

# oc wait --for=condition=complete alone just times out on a job that
# already permanently failed, condition=Failed never becomes true for
# that flag. polls both terminal conditions instead
wait_agents_job() {
  local name="$1" ns="$2" timeout="${3:-600}"
  local elapsed=0 status
  while (( elapsed < timeout )); do
    status="$(oc get "job/${name}" -n "${ns}" -o jsonpath='{.status.conditions[?(@.status=="True")].type}' 2>/dev/null || true)"
    case "${status}" in
      *Complete*) return 0 ;;
      *Failed*) return 1 ;;
    esac
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo "job ${name} in ${ns} did not reach a terminal state within ${timeout}s" >&2
  return 1
}

# builds the openshell cli image, renders this project's own three
# openshell config files (same render_openshell_file as before, pure
# sed, no openshell binary needed on the installer's own machine for
# this part), hands them to the job as a configmap, and runs the real
# sandbox create / provider import / policy set sequence as a Job
# against the shared gateway service's own in cluster dns name. nothing
# here touches the installer's own machine's network beyond oc itself
run_agents_deploy_job() {
  log "building the openshell cli image in cluster"
  oc apply -f "${AGENTS_MANIFESTS}/openshell/cli-build.yaml"
  oc start-build openshell-cli --from-dir="${ROOT_DIR}/agents" --wait -n "${AGENTS_NS}"

  local tmp
  tmp="$(mktemp -d)"
  render_openshell_file "${AGENTS_MANIFESTS}/openshell/providers/coordinator-a2a-token-grant.yaml" "${tmp}/coordinator-a2a-token-grant.yaml"
  render_openshell_file "${AGENTS_MANIFESTS}/openshell/policies/retrieval-policy.yaml" "${tmp}/retrieval-policy.yaml"
  render_openshell_file "${AGENTS_MANIFESTS}/openshell/policies/coordinator-policy.yaml" "${tmp}/coordinator-policy.yaml"
  oc create configmap spear-shield-openshell-agent-configs -n "${AGENTS_NS}" \
    --from-file="${tmp}" \
    --dry-run=client -o yaml | oc apply -f -
  rm -rf "${tmp}"

  log "checking kata-oc nodes for /dev/kvm before deciding the retrieval agent's runtime class"
  local runtime_class
  runtime_class="$(detect_agent_runtime_class)"
  if [[ -n "${runtime_class}" ]]; then
    log "kata nodes have /dev/kvm, deploying spear-retrieval under kata"
  else
    log "no usable kata node found, spear-retrieval falls back to runc"
  fi

  # not apply_templated, that helper pipes straight to oc apply with no
  # way to add this job's own extra placeholder on top, so this does
  # its own sed pass with the same two tokens plus AGENT_RUNTIME_CLASS
  delete_job_if_exists deploy-spear-sandboxed-agents "${AGENTS_NS}"
  sed \
    -e "s|MCP_PUBLIC_HOST_PLACEHOLDER|${MCP_PUBLIC_HOST}|g" \
    -e "s|GUARDRAILS_ROUTE_PLACEHOLDER|${GUARDRAILS_ROUTE}|g" \
    -e "s|MLFLOW_URL_PLACEHOLDER|${MLFLOW_URL}|g" \
    -e "s|AGENT_RUNTIME_CLASS_PLACEHOLDER|${runtime_class}|g" \
    "${AGENTS_MANIFESTS}/openshell/deploy-agents-job.yaml" | oc apply -f -

  log "waiting on job/deploy-spear-sandboxed-agents"
  if ! wait_agents_job deploy-spear-sandboxed-agents "${AGENTS_NS}" 600; then
    echo "deploy-spear-sandboxed-agents job failed, its own log:" >&2
    oc logs "job/deploy-spear-sandboxed-agents" -n "${AGENTS_NS}" --all-containers || true
    exit 1
  fi
  oc logs "job/deploy-spear-sandboxed-agents" -n "${AGENTS_NS}" --all-containers || true

  patch_sandbox_restart_policy default--spear-retrieval
  patch_sandbox_restart_policy default--spear-coordinator
}

# builds both agent images in cluster, creates the two real openshell
# sandboxes, and exposes each one's a2a listener through the gateway
# relay. the exact service names below ("a2a", "coordinator-http") and
# sandbox names ("spear-retrieval", "spear-coordinator", the 19
# character cap already found live per PHASE2_PLAN.md section 2) are
# not free choices here, manifests/spear-shield-agents/07-a2a-gateway.yaml's
# own HTTPRoute Host header rewrites already hardcode the exact strings
# openshell service expose has to print for those routes to ever match
deploy_sandboxed_agents() {
  log "building spear-coordinator-agent and spear-retrieval-agent images in cluster"
  oc apply -f "${AGENTS_MANIFESTS}/openshell/agent-builds.yaml"
  oc start-build spear-coordinator-agent --from-dir="${ROOT_DIR}/agents" --wait -n "${AGENTS_NS}"
  oc start-build spear-retrieval-agent --from-dir="${ROOT_DIR}/agents" --wait -n "${AGENTS_NS}"

  log "rag-query-relay, same image as spear-retrieval-agent, now that it actually exists"
  # openshift's internal registry refuses any cross namespace image pull
  # without an explicit grant, this deployment lives in spear-data but
  # pulls an image built in spear-shield-agents, see that rbac manifest's
  # own header comment for the live ErrImagePull this fixes
  oc apply -f "${ROOT_DIR}/manifests/spear-shield-agents/09-rag-query-relay-image-puller-rbac.yaml"
  oc apply -f "${ROOT_DIR}/manifests/spear-data/02-rag-query-relay.yaml"
  wait_deploy_ready spear-shield-rag-query-relay "${DATA_NS}"

  log "deploying the two sandboxed agents, as a real in cluster job, see run_agents_deploy_job"
  run_agents_deploy_job

  log "sandboxed agents deployed"
  oc get sandbox -n "${AGENTS_NS}"
}

main() {
  command -v oc >/dev/null 2>&1 || { echo "oc is required" >&2; exit 1; }
  command -v openssl >/dev/null 2>&1 || { echo "openssl is required" >&2; exit 1; }

  # both already applied once by scripts/07-install-guardrails.sh's own
  # ensure_early_gateway, the nemoguardrails cr needs the gateway object
  # to exist before this script ever runs, see that script's comment.
  # harmless no-ops here on a normal full chain run, kept so this script
  # still stands on its own if ever run in isolation
  log "spear-shield-agents namespace, keycloak realm import"
  oc apply -f "${AGENTS_MANIFESTS}/00-namespace.yaml"
  oc apply -f "${AGENTS_MANIFESTS}/01-keycloak-realm.yaml"

  log "spire idp and clients-federated auth flow, live admin api only, not expressible in the realm import above"
  ensure_spiffe_federated_auth

  log "spear-worktracker namespace"
  oc get namespace "${WORKTRACKER_NS}" >/dev/null 2>&1 || oc create namespace "${WORKTRACKER_NS}"

  log "openproject db and app secrets, generated once, never regenerated against an already running instance"
  ensure_secret spear-openproject-db-creds "${WORKTRACKER_NS}" \
    --from-literal=user=openproject \
    --from-literal=password="$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)" \
    --from-literal=dbname=openproject
  ensure_secret spear-openproject-app-secrets "${WORKTRACKER_NS}" \
    --from-literal=secret_key_base="$(openssl rand -hex 32)"
  # admin-password is read straight into OPENPROJECT_SEED_ADMIN_USER_PASSWORD
  # by 02-openproject.yaml below, has to exist before that manifest applies.
  # the two persona passwords are not generated here on purpose, see
  # seed_openproject_demo_data, that step owns generating and persisting
  # those itself, the same way it already owns minting api tokens, so it
  # never depends on a key existing in this secret ahead of it
  ensure_secret spear-openproject-login-creds "${WORKTRACKER_NS}" \
    --from-literal=admin-username=admin \
    --from-literal=admin-password="$(gen_openproject_password)"
  ensure_secret spear-openproject-mcp-backend-secret "${WORKTRACKER_NS}" \
    --from-literal=token="$(openssl rand -hex 24)"
  oc label secret spear-openproject-mcp-backend-secret -n "${WORKTRACKER_NS}" mcp.kuadrant.io/secret=true --overwrite
  MCP_BACKEND_SECRET="$(oc get secret spear-openproject-mcp-backend-secret -n "${WORKTRACKER_NS}" -o jsonpath='{.data.token}' | base64 -d)"

  resolve_hosts

  log "openproject db, scc, and the app itself"
  oc apply -f "${WORKTRACKER_MANIFESTS}/01-openproject-db.yaml"
  wait_deploy_ready spear-openproject-db "${WORKTRACKER_NS}"
  oc apply -f "${WORKTRACKER_MANIFESTS}/03-openproject-scc.yaml"
  apply_templated "${WORKTRACKER_MANIFESTS}/02-openproject.yaml"
  wait_deploy_ready spear-openproject "${WORKTRACKER_NS}" 300s

  log "openproject demo data: personas, data-governance project, work packages, api tokens"
  seed_openproject_demo_data

  log "spear-shield-gateway, auth policies"
  apply_templated "${AGENTS_MANIFESTS}/02-gateway.yaml"
  apply_templated "${AGENTS_MANIFESTS}/03-auth-policies.yaml"

  log "spear-openproject-mcp code configmap"
  oc create configmap spear-openproject-mcp-code -n "${WORKTRACKER_NS}" \
    --from-file=server.py="${ROOT_DIR}/mcp-servers/spear-openproject-mcp/server.py" \
    --dry-run=client -o yaml | oc apply -f -

  log "spear-openproject-mcp: MCPServer, HTTPRoute, MCPServerRegistration, MCPVirtualServer"
  apply_templated "${WORKTRACKER_MANIFESTS}/04-openproject-mcp.yaml"
  oc wait mcpserver spear-openproject-mcp -n "${WORKTRACKER_NS}" --for=condition=Ready --timeout=120s

  # guardrails rbac for both spear-openproject-mcp's and spear-coordinator's
  # own service accounts already applied by scripts/07-install-guardrails.sh
  # (manifests/spear-guardrails/07-guard-proxy-rbac.yaml,
  # 08-coordinator-caller-rbac.yaml), not repeated here

  log "spear-shield-mcp-guard-proxy code configmap, deployment, service, referencegrant for its new cross namespace backendref"
  oc create configmap spear-shield-mcp-guard-proxy-code -n "${AGENTS_NS}" \
    --from-file=server.py="${ROOT_DIR}/mcp-servers/spear-shield-mcp-guard-proxy/server.py" \
    --dry-run=client -o yaml | oc apply -f -
  apply_templated "${AGENTS_MANIFESTS}/04-mcp-guard-proxy.yaml"
  oc apply -f "${AGENTS_MANIFESTS}/05-mcp-guard-proxy-referencegrant.yaml"
  # a configmap update alone never reaches an already running pod, only a
  # fresh pod mounts the new content, so force one on every run rather
  # than silently serving stale server.py against a cluster that is not
  # a brand new install
  oc rollout restart deployment/spear-shield-mcp-guard-proxy -n "${AGENTS_NS}"

  log "spear-coordinator-agent's own credential for calling spear-guardrails directly, a dedicated sa plus a long lived token (sandboxes never automount one, confirmed live)"
  oc apply -f "${AGENTS_MANIFESTS}/openshell/coordinator-guardrails-sa.yaml"

  log "each sandbox's own credential for real in process mlflow tracing, PHASE3_PLAN.md section 4"
  oc apply -f "${AGENTS_MANIFESTS}/openshell/agents-mlflow-sa.yaml"
  oc apply -f "${ROOT_DIR}/manifests/spear-pipelines/06-agents-mlflow-rbac.yaml"

  log "spear-shield-a2a-gateway, cluster spiffeid for the two sandboxed agents"
  oc apply -f "${AGENTS_MANIFESTS}/06-cluster-spiffeid.yaml"
  apply_templated "${AGENTS_MANIFESTS}/07-a2a-gateway.yaml"

  log "console backend's own cross namespace rbac to read/exec into the two sandbox pods, applied here too since this is where the target pods live"
  oc apply -f "${AGENTS_MANIFESTS}/08-console-backend-rbac.yaml"

  log "ext_proc body mode fix (see comment on patch_mcp_ext_proc_buffered)"
  patch_mcp_ext_proc_buffered

  log "sandboxed agents: image builds, rag-query-relay, openshell sandbox create/policy/provider"
  deploy_sandboxed_agents

  cat <<EOF

spear-shield-agents and spear-worktracker deployed. resolved for this cluster, nothing hardcoded:
  openproject:  https://${OPENPROJECT_HOST}
  mcp gateway:  https://${MCP_PUBLIC_HOST}/mcp
  sso issuer:   ${ISSUER_URL}

credentials, none of them in this repo, all in cluster secrets:
  openproject admin login:         oc get secret spear-openproject-login-creds -n ${WORKTRACKER_NS} -o jsonpath='{.data.admin-password}' | base64 -d
  balakrishnan.b openproject login: oc get secret spear-openproject-login-creds -n ${WORKTRACKER_NS} -o jsonpath='{.data.balakrishnan\.b-password}' | base64 -d
  siddhartha.de openproject login:  oc get secret spear-openproject-login-creds -n ${WORKTRACKER_NS} -o jsonpath='{.data.siddhartha\.de-password}' | base64 -d
  balakrishnan.b api token:         oc get secret spear-openproject-api-tokens -n ${WORKTRACKER_NS} -o jsonpath='{.data.balakrishnan\.b}' | base64 -d
  siddhartha.de api token:          oc get secret spear-openproject-api-tokens -n ${WORKTRACKER_NS} -o jsonpath='{.data.siddhartha\.de}' | base64 -d
  spear-shield-agents realm users:  oc get keycloakrealmimport spear-shield-agents-realm -n keycloak -o jsonpath='{range .spec.realm.users[*]}{.username}{" "}{.credentials}{"\n"}{end}'

sandboxed agents, openshell sandbox list for current status:
  spear-coordinator, spear-retrieval, both created by deploy_sandboxed_agents above

one real thing worth checking after this, not assumed: manifests/spear-shield-agents/06-cluster-spiffeid.yaml
pins a literal agents.x-k8s.io/sandbox-name-hash value for the coordinator sandbox,
confirmed live on an earlier cluster to be deterministic from the sandbox name alone,
but never independently reconfirmed on this one. compare it against the real pod:
  oc get pod -n ${AGENTS_NS} -l app.kubernetes.io/managed-by=openshell -o jsonpath='{.items[0].metadata.labels.agents\.x-k8s\.io/sandbox-name-hash}{"\n"}'
and patch 06-cluster-spiffeid.yaml's own podSelector value if it differs.
EOF
}

main "$@"
