# stateful-ci

![Status](https://img.shields.io/badge/status-early%20development-f59e0b?style=flat-square)

> Experimental: This is an early release of stateful-ci. APIs, config, and behavior may change.

stateful-ci gives GitHub Actions jobs a persistent workspace across ephemeral runners.

It restores selected paths before a job starts, runs your normal CI steps, then saves the resulting workspace state for the next run. The backend runs in your own Cloudflare account.

In practice:

- The runner can still be disposable, but the workspace state does not have to be.
- You choose which paths are part of the workspace: package stores, build outputs, framework caches, generated files, browser assets, or anything else your project needs.
- Snapshots include provenance, so state from untrusted runs cannot become trusted release or deploy state.

## How it works

```
┌───────────────────────┐
│ GitHub Actions runner │
└──────────┬────────────┘
           │
           │ restore
           ▼
┌───────────────────────┐
│   stateful-ci CLI     │
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│  Cloudflare Worker    │
└──────────┬────────────┘
           │
           ├── Durable Object
           │   coordinates workspace state and snapshot commits
           │
           ├── D1
           │   stores runs, snapshots, metadata, and decisions
           │
           └── R2
               stores workspace snapshot data
```

A run has two phases:

```
restore selected workspace paths
└─ run normal CI commands
   └─ save selected workspace paths
      └─ next run can restore from that snapshot
```

## Configuration

Start with a preset:

```json
{
  "preset": "node"
}
```

Or choose paths directly:

```json
{
  "paths": ["node_modules", ".pnpm-store", ".turbo", ".next/cache"],
  "exclude": ["coverage"]
}
```

## Usage

Initialize it in your repo:

```bash
bunx stateful-ci init
```

Deploy the backend:

```bash
bunx stateful-ci deploy
```

Use it in GitHub Actions:

```yaml
- uses: eersnington/stateful-ci@v1
  with:
    command: restore

- run: bun install
- run: bun test

- uses: eersnington/stateful-ci@v1
  if: always()
  with:
    command: save
```

Inspect local state and published CI runs with TUI (OpenTUI):

```bash
bunx stateful-ci
```

## Architecture

stateful-ci has three main pieces:

| Piece                    | Role                                                                                           |
| ------------------------ | ---------------------------------------------------------------------------------------------- |
| `stateful-ci` CLI        | Runs locally and inside GitHub Actions. Restores, saves, deploys, and opens the TUI dashboard. |
| Cloudflare Worker        | Receives restore/save requests and routes workspace operations.                                |
| Durable Object + D1 + R2 | Coordinates snapshot state, records metadata, and stores workspace data.                       |

The Cloudflare backend is deployed to your own account. There is no hosted service required.

## Security model

Persistent workspace state needs provenance.

stateful-ci records where each snapshot came from and separates state by trust boundary.

```
trusted branch snapshot
├─ can seed trusted jobs
└─ can seed pull request jobs

untrusted pull request snapshot
├─ can be reused by that pull request
└─ cannot become state for trusted branches, releases, or deploy jobs
```

The goal is workspace continuity without turning persistent state into a cache-poisoning path.

## License

Apache-2.0
