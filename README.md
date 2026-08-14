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

The whole monorepo has been migrated — 123 tests green in total. The core port
is a faithful translation of the TypeScript runtime: context attribute
interception mirrors the root `Proxy` handler, `ctx.effect()` supports
sync/async disposers and generators, fibers manage load/unload/reload states
with the same inertia locking, services are traceable (caller context,
shadows, association), and events support emit/parallel/serial/bail/waterfall
with per-context filters. The loader ports the config trees (`cordis.yml`-style
entries, groups, isolation realms, intercept chains), the include package adds
YAML/JSON config files with patches, HMR swaps in `importlib.reload` for
Node's internal ESM loader (event-driven via `watchdog` — install the
`cordis[hmr]` extra — with an mtime-polling fallback), and create-cordis
fetches templates from the PyPI JSON API (`-t/--template`, `-r/--ref`,
`-m/--mirror`, `-p/--prod`) with a bundled offline template as the default.
