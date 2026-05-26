# GitHub OIDC Review Comment Explainer

This note explains the three low-severity review comments on PR #26 and the design choices behind the source comments added in response.

## 1. Per-Isolate JWKS Cache

JWKS means JSON Web Key Set. For this project, it is GitHub's public signing keys for GitHub Actions OIDC tokens.

The Worker verifies a GitHub OIDC JWT like this:

```txt
GitHub OIDC JWT
  |
  | header.kid says which GitHub signing key was used
  v
Worker loads GitHub JWKS
  |
  | find key with matching kid
  v
crypto.subtle.verify(signature)
  |
  v
VerifiedGitHubActionsIdentity
```

The verifier keeps a module-level JWKS cache:

```txt
Cloudflare Worker runtime
|
|-- isolate A
|     cachedJwks = GitHub public keys for up to 5 minutes
|
|-- isolate B
|     cachedJwks = separate copy, if that isolate has fetched keys
|
|-- isolate C
      cachedJwks = separate copy, if that isolate has fetched keys
```

Per-isolate means the cache lives only in the JavaScript isolate that handled a request. It is not durable storage, not globally shared, and not a secret store. Cloudflare may reuse an isolate for later requests, so the module-level variable can avoid refetching GitHub's public keys on every request.

The cache is safe because:

```txt
Only public signing keys are cached
  +
Cache expires after 5 minutes
  +
Verifier forces a JWKS refresh when token header.kid is missing from cache
```

Key rotation flow:

```txt
Token has kid = new-key
  |
  v
Cached JWKS only has old-key
  |
  v
Verifier fetches fresh JWKS from GitHub
  |
  v
If new-key is now published, verification can continue
```

The CodeRabbit suggestion to use Cloudflare Cache API was not adopted because the current cache is simpler, bounded, and sufficient. Moving to Cache API would add runtime coupling and more test surface without fixing a demonstrated correctness issue.

## 2. Why Verified Pull Requests Stay External

The comment added near pull request classification says:

```ts
// GitHub OIDC does not verify PR head repository identity, so PRs stay
// external until a separate trusted PR metadata source is added.
return "external";
```

The core issue is that GitHub's OIDC token proves the workflow identity, but it does not include a documented claim that identifies the pull request head repository.

GitHub's OIDC reference documents claims such as:

```txt
repository
repository_owner
event_name
ref
base_ref
head_ref
run_id
sha
workflow
workflow_ref
job_workflow_ref
```

References:

- GitHub OIDC concepts: `https://docs.github.com/en/actions/concepts/security/openid-connect`
- GitHub OIDC claim reference: `https://docs.github.com/en/actions/reference/openid-connect-reference`

The relevant documented PR claims are:

```txt
base_ref = target branch of the pull request
head_ref = source branch of the pull request
event_name = pull_request
repository = repository from where the workflow is running
```

That is not enough to prove whether the PR source branch came from the same repository or a fork.

Example:

```txt
Base repository: eersnington/stateful-ci
Base branch: main

Same-repo PR:
  head_ref: feature-branch
  actual head repository: eersnington/stateful-ci

Fork PR:
  head_ref: feature-branch
  actual head repository: attacker/stateful-ci
```

From OIDC alone, both cases can look like:

```txt
event_name: pull_request
repository: eersnington/stateful-ci
base_ref: main
head_ref: feature-branch
```

The signed OIDC token gives a source branch name, not a trusted source repository identity:

```txt
Signed OIDC claims
|
|-- event_name: yes, verified
|-- repository: yes, verified
|-- base_ref: yes, verified
|-- head_ref: yes, verified branch name
|-- pull request head repository: not available as a documented claim
|
v
Cannot safely distinguish same-repo PR from fork PR
|
v
Classify pull_request as external
```

This is intentionally conservative. It prevents a fork PR from receiving internal-cache trust just because it can produce a valid GitHub OIDC token for a workflow run.

To safely classify a PR as internal later, the Worker would need another trusted data source:

```txt
Option A: GitHub API lookup
  verified run/check context
    -> GitHub API
    -> PR metadata
    -> trusted head.repo.full_name
    -> compare with base repository

Option B: Verified webhook metadata
  GitHub webhook signature verified
    -> persist PR head repository metadata
    -> classifier reads server-side metadata
```

Until one of those exists:

```txt
verified OIDC + pull_request = external
```

## 3. OIDC-Audited Commit Producer Matching

The comment added in commit target validation says:

```ts
// OIDC-audited commits carry verified producer context, so only they can be
// matched against the producer fields stored with the prepared target.
if (
  input.auditPayloadJson !== null &&
  input.auditPayloadJson !== undefined &&
  !producerMatchesTarget(target, input.producer)
) {
  return "save_run_context_mismatch";
}
```

The save flow is two-phase:

```txt
prepare-save
  |
  | verifies OIDC token
  | derives producer context from signed claims
  | stores prepared workspace target
  v
commit-save
  |
  | verifies OIDC token again
  | checks object plan and manifest
  | confirms commit matches the prepared target
  v
snapshot becomes latest
```

The coordinator always checks stable target identity:

```txt
target.runId     == input.producer.runId
target.namespace == input.target.namespace
target.refName   == input.target.refName
```

For OIDC-audited requests, it can also compare stronger producer context:

```txt
actor
event
job
ref
repository
sha
workflow
```

Visual model:

```txt
prepare-save stores WorkspaceTarget
|
|-- runId
|-- namespace
|-- refName
|-- producerActor
|-- producerEvent
|-- producerRef
|-- producerRepository
|-- producerSha
|-- producerWorkflow

commit-save sends CommitSaveInput
|
|-- producer.runId
|-- target.namespace
|-- target.refName
|-- producer.actor
|-- producer.event
|-- producer.ref
|-- producer.repository
|-- producer.sha
|-- producer.workflow
```

Validation flow:

```txt
Do runId, namespace, and refName match?
  |
  |-- no  -> deny save_run_context_mismatch
  |
  '-- yes -> continue

Does commit include OIDC audit payload?
  |
  |-- no  -> stop at stable target checks
  |
  '-- yes -> compare verified producer fields too
              |
              |-- mismatch -> deny save_run_context_mismatch
              '-- match    -> commit can proceed
```

The stricter producer comparison is gated by `auditPayloadJson` because that payload is the signal that the request went through the verified OIDC path and carries reliable producer identity.

In other words:

```txt
OIDC token verified
  |
  v
Worker derives identity from signed GitHub claims
  |
  v
Worker creates audit payload
  |
  v
Coordinator can trust producer fields enough for strict prepare/commit matching
```

Without an OIDC audit payload, producer fields may be compatibility or request-shaped context. They should not be treated as equivalent to signed identity.

## Summary

```txt
JWKS cache
  = per-isolate public-key performance cache with TTL and kid-refresh

pull_request trust
  = external because OIDC lacks trusted PR head repository identity

commit producer matching
  = strict only when OIDC audit context proves producer identity
```
