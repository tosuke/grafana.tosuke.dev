# Repository Guidelines

## Project Structure & Module Organization

Application code lives in `src/`. `src/index.ts` is the Cloudflare Worker entry point, `src/app.ts` defines the Hono router, and `src/grafana.ts` manages the Grafana container and its outbound Litestream WebDAV proxy. WebDAV behavior is implemented in `src/lib/webdav.ts`, with integration tests beside it in `src/lib/webdav.test.ts`.

Runtime infrastructure is defined by `wrangler.jsonc` (Worker, Durable Object, container, and R2 bindings) and `Dockerfile` (Grafana plus Litestream). Generated Cloudflare bindings are stored in `worker-configuration.d.ts`; regenerate them instead of editing them manually.

## Build, Test, and Development Commands

Use pnpm 11, as declared in `package.json`.

- `pnpm install` installs locked dependencies.
- `pnpm dev` starts the local Wrangler development server.
- `pnpm test` runs the Vitest suite once in the Cloudflare Workers test pool.
- `pnpm lint` checks formatting, Oxlint rules, and TypeScript types.
- `pnpm fix` applies supported Oxfmt and Oxlint fixes.
- `pnpm generate` refreshes `worker-configuration.d.ts` from Wrangler bindings.

Run `pnpm lint && pnpm test` before submitting changes.

## Coding Style & Naming Conventions

Follow `.editorconfig`, `.oxfmtrc.json`, and `.oxlintrc.json`; let Oxfmt determine whitespace and line wrapping. TypeScript is strict, uses ES modules, and enables unchecked-index and exact-optional-property checks. Prefer explicit types at API boundaries, named exports for reusable modules, `camelCase` for functions and variables, and `PascalCase` for classes. Keep Cloudflare binding names uppercase, matching `wrangler.jsonc`.

## Testing Guidelines

Tests use Vitest with `@cloudflare/vitest-pool-workers`. Place tests next to their implementation and name them `*.test.ts`. Exercise observable HTTP behavior—status, headers, body, and R2 side effects—and clean up any bucket keys created by a test. No coverage threshold is currently enforced; add regression tests for behavior changes.

## Commit & Pull Request Guidelines

Recent history uses short imperative subjects, including Conventional Commit prefixes such as `feat:`. Keep each commit focused and use a concise subject like `fix: preserve WebDAV content type`. Pull requests should explain the user-visible or operational impact, list validation commands, and call out changes to bindings, migrations, container images, or generated files. Link relevant issues; include screenshots only for Grafana UI changes.

## Security & Configuration

Do not commit Wrangler secrets, credentials, or production data. Keep bucket and Durable Object bindings synchronized with generated types, and review dependency or container-version updates for compatibility with the pinned Grafana and Litestream setup.
