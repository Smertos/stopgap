# Stopgap + PLTS for PostgreSQL

Run TypeScript/JavaScript inside PostgreSQL, then deploy and roll back function bundles with database-native workflows.

- `plts` gives you `LANGUAGE plts` and artifact compile/store APIs.
- `stopgap` gives you versioned deploy, activation, rollback, and live function materialization.

## Who this is for

Use this project if you want to:

- Author Postgres functions in TypeScript/JavaScript.
- Keep deployment history and rollback controls in the database.
- Expose a stable live schema (default: `live_deployment`) while iterating in a source schema.

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

## Fast start: one deployable function

Create a source schema and a deployable function (`(args jsonb) returns jsonb language plts`):

```sql
CREATE SCHEMA IF NOT EXISTS app;

CREATE OR REPLACE FUNCTION app.get_user(args jsonb)
RETURNS jsonb
LANGUAGE plts
AS $$
import { query, v } from "@stopgap/runtime";

const schema = v.object({ id: v.int() });

export default query(schema, async (args, ctx) => {
  const rows = await ctx.db.query(
    "SELECT id, email FROM app.users WHERE id = $1",
    [args.id]
  );
  return rows[0] ?? null;
});
$$;
```

Deploy it to an environment:

```sql
SELECT stopgap.deploy('prod', 'app', 'initial');
```

Call the live function from the live schema:

```sql
SELECT live_deployment.get_user('{"id": 1}'::jsonb);
```

Check status/history:

```sql
SELECT stopgap.status('prod');
SELECT stopgap.deployments('prod');
SELECT stopgap.diff('prod', 'app');
```

Rollback if needed:

```sql
SELECT stopgap.rollback('prod', 1, NULL);
```

## Runtime behavior at a glance

- Entrypoint is the module `default` export.
- `ctx.args` contains decoded function arguments.
- `ctx.db.query(...)` and `ctx.db.exec(...)` run in the same transaction as the SQL call.
- `stopgap.query(...)` runs read-only (`ctx.db.mode = 'ro'`); `db.exec(...)` is denied.
- `stopgap.mutation(...)` and regular `plts` handlers run read-write (`ctx.db.mode = 'rw'`).
- JS `undefined` and `null` normalize to SQL `NULL`; other values become `jsonb`.

## CLI (optional)

Use the CLI if you prefer command-line deploy flows over raw SQL:

```bash
cargo run -p stopgap-cli -- --db "$STOPGAP_DB" deploy --env prod --from-schema app --label initial
cargo run -p stopgap-cli -- --db "$STOPGAP_DB" status --env prod
cargo run -p stopgap-cli -- --db "$STOPGAP_DB" rollback --env prod --steps 1
```

## Using the extensions

For stopgap, the CLI is the recommended interface for deployment operations. Direct SQL calls to extension functions are available but the CLI provides better ergonomics and validation.

### Workflow

1. **Create functions in a source schema** - Define your `(args jsonb) returns jsonb language plts` functions in a development schema (e.g., `app`).

2. **Deploy via CLI** - Push the source schema to an environment:
   ```bash
   stopgap-cli deploy --env prod --from-schema app --label v1.0
   ```

3. **Query live functions** - Call deployed functions through the live schema:
   ```sql
   SELECT live_deployment.get_user('{"id": 1}'::jsonb);
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
- `stopgap.deploy(env text, from_schema text, label text)`
- `stopgap.status(env text)`
- `stopgap.deployments(env text)`
- `stopgap.diff(env text, from_schema text)`
- `stopgap.rollback(env text, steps integer, to_id bigint)`

## Docs

- Architecture/source of truth: `docs/PROJECT-OUTLINE.md`
- Implementation status/backlog: `docs/ROADMAP.md`
- Runtime contract: `docs/RUNTIME-CONTRACT.md`
- Deployment lifecycle and CLI: `docs/DEPLOYMENT-RUNBOOK.md`
- Troubleshooting: `docs/TROUBLESHOOTING.md`
- Local source build/dev setup: `docs/DEVELOPER-QUICKSTART.md`
