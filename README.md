# cordis (Python)

Meta-Framework for Modern Applications — a pure Python port of
[cordiverse/cordis](https://github.com/cordiverse/cordis).

Cordis is a dependency-injection plugin framework built around a single
`Context` object:

```python
from cordis import Context
import asyncio

async def main():
    ctx = Context()

    # install a plugin — a plain function, an object with `apply()`, or a class
    await ctx.plugin(lambda ctx, config: print('hello,', config['name']), {'name': 'cordis'})

    # events
    ctx.on('ready', lambda: print('ready!'))
    ctx.emit('ready')

asyncio.run(main())
```

## Features

- **Plugin system** — functional, object, and class plugins with lifecycle
  management (`Fiber`s with load / unload / reload states).
- **Dependency injection** — `ctx.provide()`, `ctx.inject()`, `ctx.get()`,
  `ctx.set()`, `ctx.isolate()`, `ctx.intercept()`, `Service` base class, and
  the `@Inject` decorator.
- **Reactive effects** — `ctx.effect()` with sync / async disposers and
  generators; every side effect is bound to a fiber and cleaned up on dispose.
- **Events** — `emit`, `parallel`, `serial`, `bail`, `waterfall`, `on`, `once`
  with per-context filtering.
- **Logging** — a `logger` service with pluggable exporters and format strings.

## Development

```bash
uv sync          # install the project + test dependencies
uv run pytest    # run the test suite
```

## Migration status

| JS package (cordiverse/cordis) | Python module | Status |
| --- | --- | --- |
| `packages/core` | `cordis` (`src/cordis/`) | ✅ ported, 72 tests green |
| `packages/loader` | `cordis.loader` | ✅ ported (Entry/Group/Tree/Loader/isolate) |
| `packages/include` | `cordis.include` | ✅ ported (YAML/JSON trees + patches) |
| `packages/timer` | `cordis.timer` | ✅ ported, 14 tests green |
| `packages/logger-console` | `cordis.logger_console` | ✅ ported, 5 tests green |
| `packages/group` | `cordis.loader.Group` | ✅ ported |
| `packages/utils` | `cordis.utils.List` | ✅ ported |
| `packages/hmr` | `cordis.hmr` | ✅ ported (watchdog watcher + polling fallback) |
| `packages/create` | `cordis.create` | ✅ ported (registry template fetch + bundled template) |

The whole monorepo has been migrated — 156 tests green in total. The core port
is a faithful translation of the TypeScript runtime: context attribute
interception mirrors the root `Proxy` handler, `ctx.effect()` supports
sync/async disposers and generators, fibers manage load/unload/reload states
with the same inertia locking, services are traceable (caller context,
shadows, association), and events support emit/parallel/serial/bail/waterfall
with per-context filters. The loader ports the config trees (`cordis.yml`-style
entries, groups, isolation realms, intercept chains), the include package adds
YAML/JSON config files with patches, HMR swaps in `importlib` for Node's
internal ESM loader (event-driven via `watchdog` — install the `cordis[hmr]`
extra — with an mtime-polling fallback), and create-cordis fetches templates
from the PyPI JSON API (`-t/--template`, `-r/--ref`, `-m/--mirror`,
`-p/--prod`) with a bundled offline template as the default.

## Faithfulness notes (vs. the Cordis paper)

The port follows the TypeScript runtime wherever the paper and the runtime
disagree; the known semantic gaps, all inherited from the JS implementation,
are recorded here for anyone auditing against the paper:

- **Unload is concurrent across a fiber's effects** (`asyncio.gather` mirrors
  the runtime's `Promise.all`), not the sequentially LIFO-composed accumulator
  of the paper's Algorithm 5. LIFO ordering is preserved only within a single
  effect's yielded disposers.
- **The withdrawal guard sits inside the `provide` disposer**, not ahead of
  the whole recovery as in Algorithm 5 line 25 — so a provider's *other*
  effects may tear down while its dependents are still unloading. The
  paper's readability guarantee (the coeffect binding stays readable
  throughout a dependent's own teardown) does hold.
- **Failed fibers re-enter the lifecycle** when their target digest changes
  (e.g. a dependency is re-provided by a new fiber), where the paper's
  `L-Begin` premise (start only from `Inactive(⊥)`) withholds them.
- **A loader entry whose `name` changes is not rebuilt** — only `config`
  changes (and groups) reload the fiber; the paper's per-field dispatch says
  id/url changes rebuild the entry.
- **Intercept metadata merges leaf-first**: a component's own inject config
  overrides enclosing `ctx.intercept()` metadata, the opposite bias of the
  paper's Definition 31.
- **HMR is transactional but not graph-aware**: the JS engine classifies the
  import graph and transitively invalidates stale entries; Python's
  `importlib` cannot rebind importers, so only the entries whose own module
  changed are reloaded (with module-cache backup and rollback on failure).

## The paper as a test suite

`tests/paper/` renders the paper's formal content as executable tests, in
three tiers: `test_theorems.py` checks the Section 3 algebra of revertible
effects against a small self-contained reference model (exhaustively on
finite state spaces); `test_calculus.py` stages each of the ten lifecycle
rules of Section 4 against the real runtime (the two known paper-vs-runtime
deviations above appear as `xfail(strict=True)` with citations); and
`test_metatheory.py` replays seeded random plugin programs, checking the
preservation / termination / confluence guarantees of Section 4.4 after
every step. Every module is written to be readable without a PL background.
Run it on its own with `uv run pytest tests/paper`.
