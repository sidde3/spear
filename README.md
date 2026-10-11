# SPEAR Shield

An ABAC/RBAC-enforced RAG + agent platform on OpenShift AI: guardrailed retrieval over a classified document corpus, two sandboxed agents (coordinator + retrieval) talking over SPIFFE-authenticated A2A, a work tracker integration with its own tool-level RBAC, AutoRAG-driven pipeline optimization, and an evaluation layer (ragas/ibm-clear/garak/confidential-data-leak scanning).

This document exists because the first real install of this project on a fresh cluster hit a long list of issues, most of them not bugs in the application logic but sequencing, timing, and external-dependency traps that are easy to hit once and should never need rediscovering. Everything below reflects what was actually confirmed live, not assumed.

## 1. Prerequisites

### Cluster

- OpenShift 4.20.x. The console plugin's `@console/pluginAPI` version is pinned to match this exactly — see [Known issue: console plugin invisible](#console-plugin-invisible) if you're on a different minor version.
- At least one node pool labelable `kata-oc` for the sandboxed agents (`scripts/00-a-install-platform.sh` labels and reboots this pool; expect the API server to drop briefly during that step).
- Cluster-admin access. Several steps touch cluster-scoped objects (`OAuth`, `ClusterRole`s, operator subscriptions).

### Local tooling

| Tool | Used for |
|---|---|
| `oc` (logged in as cluster-admin) | everything |
| `python3` | install scripts' own JSON handling, and the pipeline trigger scripts |
| `curl`, `openssl` | install scripts |
| `kfp` SDK, pinned | triggering/registering KFP pipelines — see [.venv-kfp setup](#venv-kfp-setup) below, **do this before `04-install-pipelines.sh`** |
| Node.js 22+ with Corepack (or a global `yarn`) | building the console's frontend plugin in `08-install-console.sh`. If `yarn` isn't on `PATH`, use `corepack yarn install` / `corepack yarn build` instead of a global install — this avoids needing write access to `/usr/local/bin`, which isn't guaranteed on every machine |

<a id="venv-kfp-setup"></a>
**`.venv-kfp` setup**, required before `04-install-pipelines.sh` (otherwise `phase1-apply-pattern-pipeline` registration silently soft-skips with a warning instead of failing — easy to miss):

```
python3 -m venv .venv-kfp
.venv-kfp/bin/pip install "kfp[kubernetes]==2.17.0"
```

The version is pinned to match `owner-console-backend/requirements.txt`'s own `kfp==2.17.0`, so the compiled pipeline IR stays consistent with what the console backend expects.

### Credentials to obtain before you start

Three API keys are needed for `scripts/05-install-inference.sh`, and the script deliberately does **not** create them for you — the comment at the top of that script explains why: so a raw key never has to pass through an assistant, a chat transcript, or get typed into anything other than your own terminal. Create them yourself, in your own terminal, before running that script:

```
oc create secret generic ogx-glm-key -n spear-inference \
  --from-literal=apiKey='<the maas gateway key for glm-53-flash>'
oc label secret ogx-glm-key -n spear-inference ogx.io/watch=true

oc create secret generic ogx-litemaas-key -n spear-inference \
  --from-literal=apiKey='<the litemaas gateway key, scoped to the chat model>'
oc label secret ogx-litemaas-key -n spear-inference ogx.io/watch=true

oc create secret generic ogx-embedding-key -n spear-inference \
  --from-literal=apiKey='not-required-auth-disabled-on-tei'
oc label secret ogx-embedding-key -n spear-inference ogx.io/watch=true
```

The third one is a placeholder, not a real credential — the self-hosted embedding server has no auth of its own.

**Known risk, not fixable from this repo:** the `ogx-litemaas-key` gateway (`litemaas.rhoai.rh-aiservices-bu.com`) has been observed to (a) silently change which model it serves behind the same key, and (b) have the key itself revoked server-side entirely (`"Unable to find token in cache or LiteLLM_VerificationTokenTable"`). If AutoRAG's `search-space-preparation` step fails with `SearchSpaceValueError: ... do not respond`, this is almost always the cause — verify with a direct completion call against `vllm-inference/<whatever model /v1/models currently lists>` through `rag-phase1-ogx-service`, and if it 401s, get a fresh key from whoever administers that gateway. In the meantime, AutoRAG can run with just `glm-53-flash` in its generation-models list (see [Triggering the RAG pipeline lifecycle](#pipeline-lifecycle)).

## 2. Install order

Run these from the repo root. Every script is idempotent — reruns against an already-installed cluster just re-apply and move on, which matters because the first attempt at several of these steps is expected to fail for reasons explained below.

```
./scripts/00-a-install-platform.sh
./scripts/00-b-install-openshell.sh
# create the three ogx-*-key secrets now, see Prerequisites above
./scripts/00-install-spear.sh          # wraps 01 through 09, then 90-verify
```

If you'd rather run the chain step by step instead of the `00-install-spear.sh` wrapper (useful if something fails partway and you want to resume), the order is:

```
./scripts/01-install-namespaces.sh
./scripts/02-install-storage.sh
./scripts/03-install-data.sh
# set up .venv-kfp now if you haven't already, see Prerequisites above
./scripts/04-install-pipelines.sh
./scripts/05-install-inference.sh
./scripts/06-setup-autorag-secrets.sh
./scripts/07-install-guardrails.sh
./scripts/08-install-console.sh
./scripts/09-install-worktracker-and-agents.sh
./scripts/90-verify.sh
```

Ignore any "next step" hint printed by `00-a-install-platform.sh` or `00-b-install-openshell.sh` pointing at `deploy-agent-pack.sh` or `31-deploy-openshell-agents.sh` on an **old** checkout — those were leftover references to a different, shared base project neither of these scripts actually belongs to, and have been corrected to point at the real next script in this repo.

### What to expect on a first run, and why

These are not bugs to work around by hand each time — each one is explained here so a first-time operator recognizes it and knows the right response, but the only ones still requiring manual action are the operator channel approvals; everything else is already handled by the scripts as committed.

**`00-a-install-platform.sh` may need two or three passes the very first time.**

1. *`error: resource mapping not found for name: "odh-dashboard-config" ... no matches for kind "OdhDashboardConfig"`.* The RHOAI operator subscription was just approved and its CRDs haven't finished registering yet when the script tries to apply the dashboard config patch a few seconds later. Just rerun the script — it's idempotent, and by the second run the CRD exists.
2. *`refusing to approve install plan ... for servicemeshoperator3 ... targets ["servicemeshoperator3.v3.4.3"], not the pinned startingCSV servicemeshoperator3.v3.4.2`* (or the same for the Agent Sandbox operator). This is intentional, not a bug — `scripts/lib-olm.sh` refuses to approve any OLM install plan that doesn't match the exact CSV pinned in `manifests/platform/00-operators.yaml`, specifically so a rerun of this script can never silently approve a channel upgrade OLM proposed on its own. You have two options:
   - If you want the newer version: log into the OpenShift console, approve the pending install plan for that operator by hand (Operators → Installed Operators), **then** bump the matching `startingCSV` in `manifests/platform/00-operators.yaml` to match, so future runs don't flag it again.
   - If you want to stay pinned: something changed upstream in the catalog; investigate before approving anything.

   Either way, rerun `00-a-install-platform.sh` after resolving it — on this project's own first install, both the Agent Sandbox and Service Mesh operators hit this and needed a manual console approval before the third run succeeded cleanly.

**`05-install-inference.sh` may print `warning: could not confirm all three model ids in /v1/models output above, inspect manually`.** `ogx` discovers its upstream models once at pod startup; if the three secrets above were created only moments before this script ran, give it a minute and rerun, or check `/v1/models` directly against `rag-phase1-ogx-service`.

**`07-install-guardrails.sh`'s smoke tests may fail on the first run with an `Application is not available` HTML page**, not a real guardrails error — that's OpenShift's own router default page, returned when a brand-new Route hasn't finished propagating through HAProxy yet. The script's own `wait_guardrails_ready` only confirms the NemoGuardrails operator's reconcile conditions, not that the router has picked up the new route. Just rerun the script; it passes cleanly once the route has settled, usually within well under a minute.

## 3. Logging into the console as a real user, not just kube:admin

`08-install-console.sh` wires OpenShift's own cluster login up to the project's Keycloak realm automatically (`ensure_openshift_oauth_idp`), so you don't need `kube:admin` to use the "Ask SPEAR Shield" chat page. After it runs, the OpenShift login page gets a new **"Log in with spear-shield-agents"** option. Log in as one of the two seeded personas:

| Username | Password | clearance_level | dept | team |
|---|---|---|---|---|
| `balakrishnan.b` | `ConsoleTest#2026` | general | engineering | data-eng |
| `siddhartha.de` | `ConsoleTest#2026` | restricted | data-governance | (none) |

This matters specifically because the console backend's chat flow maps your **OpenShift username** (not your Keycloak login) to a password file named after that exact username — `kube:admin` has no such file, and will fail with `no spear-shield-agents realm password on file for 'kube:admin'` rather than a useful chat response. If you see a generic "Access denied due to cluster policy" banner on the chat page, this is almost always the actual cause: that specific text comes from OpenShift's own console layer wrapping a 403, not from this project's backend at all.

If the SpearShield nav section itself doesn't appear after `08-install-console.sh` completes, hard-refresh the browser tab — the console only loads its plugin list once per session.

<a id="console-plugin-invisible"></a>
**Console plugin invisible even after a hard refresh:** this means the plugin's declared `@console/pluginAPI` version doesn't match this cluster's actual OpenShift minor version. The dynamic plugin loader checks this client-side in the browser and silently drops the plugin with no server-side error at all — `ConsolePlugin` registered, backend reachable, everything looks fine except the nav item never shows up. Check `oc get clusterversion version -o jsonpath='{.status.desired.version}'` and make sure `owner-console/package.json`'s `@console/pluginAPI` and `@openshift-console/dynamic-plugin-sdk(-webpack)` versions match that minor version, then rebuild and redeploy (`corepack yarn install && corepack yarn build`, rebuild the image, `helm upgrade` the `phase1-ingestion-console` release to the new digest).

<a id="pipeline-lifecycle"></a>
## 4. Triggering the RAG pipeline lifecycle (nothing does this automatically)

Three pipelines run in sequence, and **none of them run on their own** — the install scripts only register `phase1-apply-pattern-pipeline` (so `90-verify.sh` has something to check), and `documents-rag-optimization-pipeline` (AutoRAG) is auto-registered by RHOAI itself. `phase1-ingestion-pipeline` doesn't even exist on the pipeline server until you trigger it the first time. Until all three steps below have run at least once, `rag_chunks` doesn't exist, and every retrieval attempt — including the console chat page — fails with a `502 Bad Gateway` from `rag-query-relay`, saying nothing is wrong except that there's nothing to search yet.

Via the console's **Knowledge Base** page: click **Sanitize sources** (ingestion), wait for it to finish, reload the page, then **Optimize RAG parameters** (AutoRAG) becomes enabled once that batch shows "report ready." Once AutoRAG has a winner, **Apply winning pattern** appears.

Or via the CLI, after activating `.venv-kfp`:

```
python pipelines/phase1_ingestion/compile_and_run.py
python pipelines/phase1_autorag/run_autorag.py <batch_id>
python pipelines/phase1_apply_pattern/compile_and_run.py <batch_id> <autorag_run_id> <pattern_name> <chunk_size> <chunk_overlap> <embedding_model>
```

The apply-pattern parameters come straight off the AutoRAG leaderboard winner — read them from the console's own Knowledge Base page, or `GET /kfp/autorag-leaderboard?run_id=...` through the console backend, rather than guessing.

If the dead `ogx-litemaas-key` from the Prerequisites section is still unresolved when you run AutoRAG, drop `vllm-inference/<model>` from its candidate list and run with `glm-53-flash` alone — `pipelines/phase1_autorag/run_autorag.py`'s own `GENERATION_MODELS` constant can be overridden at the call site without editing the file (`run_autorag.GENERATION_MODELS = [...]` before calling `run_autorag.main()`), so the repo's own two-model intent stays intact for whenever the key is fixed.

## 5. Evaluation Trends (optional, separate from the main chain)

The "Evaluation Trends" console page reads from an MLflow experiment that only gets written to by `scripts/11-submit-evalhub-jobs.sh`, which in turn needs `scripts/10-install-evalhub.sh` to have actually been run first — neither is part of the main install chain, by design (`scripts/10-install-evalhub.sh`'s own header comment: "submitting the actual evaluation jobs is a separate, manual step"). Run some real chat questions through "Ask SPEAR Shield" first, so there's real trace data for `ragas`/`ibm-clear` to score, then:

```
./scripts/10-install-evalhub.sh
./scripts/11-submit-evalhub-jobs.sh
```

`garak`'s jailbreak probe targets the raw `glm-53-flash` model directly, bypassing guardrails — a high attack-success-rate result there is the *expected*, useful finding (it's evidence for why the guardrails layer exists), not a sign something is broken.

## 6. Verifying the install

```
./scripts/90-verify.sh
```

covers the platform, data layer, pipelines, guardrails, inference, work tracker, and console — but **not** the sandboxed agents (`spear-coordinator`, `spear-retrieval`) or the console plugin's own visibility, since neither has a clean scripted health check. Check those directly:

```
oc get sandbox -n spear-shield-agents
oc get consoleplugin phase1-ingestion-console
```

## 7. Troubleshooting runbook

### After any cluster or node restart

Two independent things can silently break, and `90-verify.sh` won't catch either:

**`412 Precondition Failed: "Sandbox is not ready"` from the chat page.** The kata-sandboxed agent pods (`default--spear-coordinator`, `default--spear-retrieval`) can come back as `ContainerStatusUnknown` after a node restart — `restartPolicy=Always` only restarts containers within a pod, it can't recover a pod whose own identity was lost. Check `oc get sandbox -n spear-shield-agents`; if `Ready: False`, delete the stuck pod and the `Sandbox` controller recreates it fresh:

```
oc delete pod default--spear-coordinator -n spear-shield-agents
oc delete pod default--spear-retrieval -n spear-shield-agents
```

**A bare `503` from any Istio gateway in `spear-shield-agents`** (the chat page, work-tracker/MCP routes, or anything else routed through `spear-shield-a2a-gateway-istio` / `spear-shield-gateway-istio`). Check that gateway pod's own logs for `critical envoy wasm: Plugin kuadrant-wasm-shim failed to load` / `Plugin configured to fail closed failed to load`. Kuadrant's own Envoy WASM filter can fail a remote fetch during the general churn right after a cluster restart, and because it's configured fail-closed, the proxy passes its own readiness probe while silently rejecting every request. WASM loads once at Envoy startup and won't retry on its own — delete the pod to force a fresh attempt once the cluster has settled:

```
oc logs <gateway-pod> -n spear-shield-agents | grep -i "critical envoy wasm"
oc delete pod <gateway-pod> -n spear-shield-agents
```

Both `spear-shield-a2a-gateway-istio` and `spear-shield-gateway-istio` can hit this independently — check both if one route works and another doesn't.

### Other symptoms

| Symptom | Real cause | Fix |
|---|---|---|
| `ImagePullBackOff` / `authentication required` pulling an internal-registry image across namespaces | OpenShift's internal registry refuses cross-namespace pulls without an explicit grant | confirm the relevant `system:image-puller` RoleBinding exists in the source namespace (already wired into the install scripts; if you hit this on a modified checkout, it means a similar cross-namespace image reference was added without the matching grant) |
| A KFP task pod stuck in `CreateContainerConfigError`, `Error: secret "X" not found` | the referenced secret only exists in a different namespace — `secretKeyRef` never resolves cross-namespace | mirror the secret into the namespace the pod actually runs in (`mirror_secret` in `scripts/lib-common.sh` is the established pattern) |
| A KFP task fails with `psycopg2.errors.SyntaxError: syntax error at or near "$"` on a `DO $$ ... $$` block | RHOAI's own KFP v2 launcher can corrupt a dollar-quoted PL/pgSQL block when reconstructing component source inside the pod at runtime — confirmed by diffing the compiled pipeline YAML and the exact version stored on the pipeline server, both correct, so the corruption happens after that | avoid dollar-quoting; do the equivalent check in Python before the `cur.execute()` call instead (see `pipelines/phase1_apply_pattern/pipeline.py`'s own `rag_reader` role check for the pattern) |
| A `403 Forbidden` from a RHOAI-vendored service sitting behind its own sidecar proxy (evalhub, etc.) | the proxy's own `kube-rbac-proxy`-style `auth.yaml` may require resource/apiGroup combinations that aren't obvious from the service's own docs, and `oc auth can-i` is unreliable for resource types the cluster can't discover (it can report "no" for a permission that's actually granted) | read the sidecar's real config directly (`oc exec <pod> -c <proxy-container> -- cat /etc/kube-rbac-proxy/auth.yaml` or equivalent) rather than guessing from docs, and verify with a raw `SubjectAccessReview` object instead of `oc auth can-i` |
| `/v1/guardrail/checks` or any freshly-created Route returns OpenShift's "Application is not available" page | router propagation race — the backing operator's own `Ready` condition doesn't guarantee HAProxy has picked up the new route yet | wait briefly and retry; if it's inside an install script, just rerun the script |
| Retrieval returns a `502` from `rag-query-relay`, or the chat page says the knowledge base search failed | `rag_chunks` doesn't exist yet | run the full pipeline lifecycle, [section 4](#pipeline-lifecycle) |
| AutoRAG's `search-space-preparation` fails with `SearchSpaceValueError: ... do not respond` for one specific model | that model's upstream gateway key is dead or the model itself changed upstream | see the LiteMaaS risk note in Prerequisites |

## 8. Security properties worth knowing, not just assuming

- `rag_chunks`' row-level-security policy normalizes every ABAC claim with `NULLIF(..., '')` before comparing it — a caller missing a department or clearance claim compares against `NULL`, which is never true, so it fails closed rather than matching rows that themselves have an empty-string column instead of `NULL`. Verify live: `select polname, pg_get_expr(polqual, polrelid) from pg_policy where polrelid='rag_chunks'::regclass`.
- Work-tracker tool access is enforced at two independent layers: the MCP gateway's own tool-level `AuthPolicy` (role-based — do you have `tool:update_work_package` at all), and a separate assignee-self check inside `spear-openproject-mcp` itself (even with the role, you can only update work packages assigned to you). A real test needs to confirm both, not just that an update was politely declined — check the response's own `trace` array for which layer actually fired (`tool-rbac` vs an explicit assignee-mismatch message), since the coordinator model can decline a tool call on its own reasoning without ever exercising the real enforcement path.
- Any RBAC fix for a Kuadrant/OpenShift-AI-vendored service should default to the narrowest scope that satisfies the real, observed check (per-namespace `Role`/`RoleBinding`), not a cluster-wide grant — a multi-tenant service's own per-request namespace header (e.g. evalhub's `X-Tenant`) is only a real isolation boundary if the underlying Kubernetes RBAC grant is namespaced too.
