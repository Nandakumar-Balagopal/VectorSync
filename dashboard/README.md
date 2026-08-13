# VectorSync Dashboard

React + TypeScript dashboard for the phase-one VectorSync prototype.

The dashboard is intentionally a thin UI over the running local services. For E2E testing, failed backend calls should be visible instead of silently replaced with fake successful search results.

## What it covers today

- Registered table overview
- Discovered table registration flow
- Manual sync trigger
- Semantic search playground
- Job, alert, debug, and pipeline pages for planned control-plane features

Some pages still use placeholder fallback data because the matching backend APIs are not implemented in phase one yet. The semantic search path is wired to the real search service.

## Local development

From the repo root, start the backend:

```bash
./scripts/start-local.sh
```

Then start the dashboard:

```bash
cd dashboard
npm install
npm run dev
```

The Vite dev server runs on:

```text
http://localhost:3000
```

## Backend proxy wiring

Vite proxies dashboard API calls to the local services:

| Frontend path | Backend service | Backend port |
| --- | --- | --- |
| `/api/*` | control-plane | `8080` |
| `/worker-api/*` | worker, rewritten to `/api/*` | `8081` |
| `/search-api/*` | search-service, rewritten to `/api/*` | `8083` |

## Important routes

- `/` — overview and table registration
- `/search` — real semantic search against the vector table
- `/config` — discovered table sync/registration workflow
- `/debug`, `/pipeline`, `/jobs`, `/alerts` — phase-two operational views

## Build

```bash
npm run build
```

If this fails with `tsc: command not found`, install dependencies first:

```bash
npm install
```
