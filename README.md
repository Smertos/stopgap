# Stopgap + PLTS for PostgreSQL

Run TypeScript/JavaScript inside PostgreSQL, then deploy and roll back function bundles with database-native workflows.

Status note (Mar 2026): product direction is being course-corrected to a Convex-style TypeScript-first workflow where app code lives in `./stopgap` and is invoked by function path (`stopgap.call_fn`).

- `plts` gives you `LANGUAGE plts` and artifact compile/store APIs.
- `stopgap` gives you versioned deploy, activation, rollback, and path-based function invocation.

## Who this is for

Use this project if you want to:

- Author Postgres functions in TypeScript/JavaScript.
- Keep deployment history and rollback controls in the database.
- Author only TypeScript modules/functions in your app repo, without hand-writing SQL function wrappers.

## Local development setup

Use pnpm for runtime package dependencies before running Cargo commands:

```bash
pnpm --dir packages/runtime install --frozen-lockfile
cargo check
```

For full local prerequisites and command references, see `docs/DEVELOPER-QUICKSTART.md`.

## Install in your database

### Option 1: Build extensions from source (multi-stage Dockerfile)

Check out example in `Dockerfile.test``



### Option 2: Install into an existing container

```bash
# Copy extension files into a running container
docker cp target/release/plts--*.so container:/usr/lib/postgresql/17/lib/
docker cp target/release/stopgap--*.so container:/usr/lib/postgresql/17/lib/
docker cp target/release/plts.control container:/usr/share/postgresql/17/extension/
docker cp target/release/stopgap.control container:/usr/share/postgresql/17/extension/

# Restart to load and create extensions
docker restart container
docker exec -it container psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS plts;"
docker exec -it container psql -U postgres -c "CREATE EXTENSION IF NOT EXISTS stopgap;"
```

After installing the extension binaries in Postgres, enable them in SQL:

```sql
CREATE EXTENSION IF NOT EXISTS plts;
CREATE EXTENSION IF NOT EXISTS stopgap;
```

## Fast start: TypeScript-first app workflow

Create project-local function modules under `./stopgap`:

```ts
// stopgap/coolApi.ts
import { query, v } from "@stopgap/runtime";

export const myFn = query(
  v.object({ id: v.int() }),
  async (args, ctx) => {
    const rows = await ctx.db.query("select id, email from app.users where id = $1", [args.id]);
    return rows[0] ?? null;
  }
);
```

Deploy from project root:

```bash
stopgap --db "$STOPGAP_DB" deploy --env prod --label initial
```

Call by function path:

```sql
SELECT stopgap.call_fn('api.coolApi.myFn', '{"id": 1}'::jsonb);
```

Check status/history:

```sql
SELECT stopgap.status('prod');
SELECT stopgap.deployments('prod');
SELECT stopgap.diff('prod');
```

Rollback if needed:

```sql
SELECT stopgap.rollback('prod', 1, NULL);
```

## Runtime behavior at a glance

- Stopgap app entrypoints are named exports resolved by `api.<module>.<export>` function path.
- Regular standalone `plts` modules continue using default-export entrypoints.
- `ctx.args` contains decoded function arguments.
- `ctx.db.query(...)` and `ctx.db.exec(...)` run in the same transaction as the SQL call.
- `stopgap.query(...)` runs read-only (`ctx.db.mode = 'ro'`); `db.exec(...)` is denied.
- `stopgap.mutation(...)` and regular `plts` handlers run read-write (`ctx.db.mode = 'rw'`).
- JS `undefined` and `null` normalize to SQL `NULL`; other values become `jsonb`.

## CLI (optional)

Use the CLI if you prefer command-line deploy flows over raw SQL:

```bash
cargo run -p stopgap-cli -- --db "$STOPGAP_DB" deploy --env prod --label initial
cargo run -p stopgap-cli -- --db "$STOPGAP_DB" status --env prod
cargo run -p stopgap-cli -- --db "$STOPGAP_DB" rollback --env prod --steps 1
```

## Using the extensions

For stopgap, the CLI is the recommended interface for deployment operations. Direct SQL calls to extension functions are available but the CLI provides better ergonomics and validation.

### Workflow

1. **Create functions in `./stopgap`** - Define named exports in `stopgap/**/*.ts` modules using `query(...)` / `mutation(...)`.

2. **Deploy via CLI** - Publish the local `stopgap/` module set to an environment:
   ```bash
   stopgap-cli deploy --env prod --label v1.0
   ```

3. **Invoke functions by path** - Call deployed handlers through `stopgap.call_fn`:
   ```sql
   SELECT stopgap.call_fn('api.coolApi.myFn', '{"id": 1}'::jsonb);
   ```

4. **Manage via CLI** - Check status, view history, or rollback:
   ```bash
   stopgap-cli status --env prod
   stopgap-cli deployments --env prod
   stopgap-cli diff --env prod
   stopgap-cli rollback --env prod --steps 1
   ```

### Environment variable

Set `STOPGAP_DB` to avoid passing `--db` on every command:
```bash
export STOPGAP_DB="postgres://user:pass@localhost:5432/mydb"
```

## Important SQL APIs

- `plts.compile_ts(source_ts text, compiler_opts jsonb)`
- `plts.compile_and_store(source_ts text, compiler_opts jsonb)`
- `plts.get_artifact(artifact_hash text)`
- `stopgap.call_fn(path text, args jsonb)`
- `stopgap.deploy(env text, label text)` (target shape during pivot; legacy signature may still exist during migration)
- `stopgap.status(env text)`
- `stopgap.deployments(env text)`
- `stopgap.diff(env text)` (target shape during pivot)
- `stopgap.rollback(env text, steps integer, to_id bigint)`

## Docs

- Architecture/source of truth: `docs/PROJECT-OUTLINE.md`
- Implementation status/backlog: `docs/ROADMAP.md`
- Runtime contract: `docs/RUNTIME-CONTRACT.md`
- Deployment lifecycle and CLI: `docs/DEPLOYMENT-RUNBOOK.md`
- Troubleshooting: `docs/TROUBLESHOOTING.md`
- Local source build/dev setup: `docs/DEVELOPER-QUICKSTART.md`
