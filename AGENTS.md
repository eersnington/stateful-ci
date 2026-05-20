# Overview

## Core Priorities

1. Performance first.
2. Reliability first.
3. Keep behavior predictable under load and during failures (session restarts, reconnects, partial streams).

If a tradeoff is required, choose correctness and robustness over short-term convenience.

## Maintainability

Long term maintainability is a core priority. If you add new functionality, first check if there is shared logic that can be extracted to a separate module. Duplicate logic across multiple files is a code smell and should be avoided. Don't be afraid to change existing code. Don't take shortcuts by just adding local logic to solve a problem.

## Testing

- Avoid mocks as much as possible as they don't reflect product behavior
- Keep unit tests minimal as possible and focused on business logic
- Test actual implementation, do not duplicate logic into tests
- Avoid done callbacks in async tests - use async/await instead
- Keep test suites reasonably flat - avoid excessive `describe` nesting

## General Principles

- Keep things in one function unless composable or reusable.
- Avoid `try`/`catch` where possible.
- Avoid using the `any` type.
- Do not extract single-use helpers preemptively. Inline the logic at the call site unless the helper is reused, hides a genuinely complex boundary, or has a clear independent name that improves the caller.
- Rely on type inference when possible; avoid explicit type annotations or interfaces unless necessary for exports or clarity.
- Prefer functional array methods (flatMap, filter, map) over for loops; use type guards on filter to maintain type inference downstream. Exceptions are allowed when the functional is NOT performant.
- Prefer early returns and guard clauses over nested `if`/`else` trees.
- Avoid hand-rolled result protocols like `{ ok: true, value } | { ok: false, error }`. Use Effect's error channel and tagged errors for fallible operations.
- Prefer discriminated unions or tagged errors that make illegal states unrepresentable instead of loosely optional objects for state.
- Export only what another package or test needs.
- Error messages should identify what failed, why it matters, and the next useful diagnostic or recovery action.

- Keep package boundaries explicit: `packages/core` is runtime-neutral shared contracts; `packages/stateful-ci` runs in GitHub Actions on Node/Bun; `packages/worker` runs in Cloudflare Workers
- In `packages/core`, do not import Node, Bun, Cloudflare, filesystem, compression, fetch, or Web Worker APIs. Core owns schemas, IDs, protocol/config types, constants, and pure domain planning
- In `packages/stateful-ci`, use explicit Node/Bun APIs for CLI and GitHub Actions behavior. Do not leak those APIs into core
- In `packages/worker`, use Cloudflare runtime APIs only where Worker behavior belongs. Do not make Worker code responsible for local snapshot production
