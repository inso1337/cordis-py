"""Hot module replacement — the Python equivalent of `@cordisjs/plugin-hmr`.

The JS package drives Node's internal ESM module loader and watches files
with chokidar.  The Python port reloads changed plugins through
`importlib.reload` + the registry, watching the file system event-driven via
`watchdog` when it is installed (extra: `cordis[hmr]`), with an mtime-polling
fallback so the zero-dependency installation keeps working.
"""

from __future__ import annotations

import fnmatch
import importlib
import os
from typing import Any, Optional

from .service import Service

try:  # optional dependency — enables the event-driven watcher
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError:  # pragma: no cover — exercised by the polling tests
    FileSystemEventHandler = None
    Observer = None

WATCHDOG_AVAILABLE = Observer is not None

_DEFAULT_IGNORED = ("**/node_modules", "**/.*", "cache", "data")


class _WatchHandler(FileSystemEventHandler):
    def __init__(self, hmr: "Hmr", is_ignored):
        self._hmr = hmr
        self._is_ignored = is_ignored

    def on_modified(self, event):
        if self._hmr._stopped or event.is_directory:
            return
        path = event.src_path
        if not self._is_ignored(path):
            self._hmr._on_file_event(path)

    def on_created(self, event):
        self.on_modified(event)

    def on_moved(self, event):
        path = event.dest_path
        if not self._is_ignored(path):
            self._hmr._on_file_event(path)


class Hmr(Service):
    inject = ["loader", "timer"]

    def __init__(self, ctx, config: Optional[dict] = None):
        super().__init__(ctx, "hmr")
        self.config = dict(config or {})
        self._snapshots: dict = {}
        self._dispose_timer = None
        self._observer = None
        self._debounced = None
        self._stashed: set = set()
        self._stopped = False

    def __cordis_init__(self):
        hmr = self

        async def gen():
            yield lambda: hmr.stop()
            hmr.start()

        return gen()

    def start(self):
        self.watch()
        if WATCHDOG_AVAILABLE:
            self._start_observer()
        else:
            self._dispose_timer = self.ctx.setInterval(self._poll, self.config.get("debounce", 100))

    def stop(self):
        self._stopped = True
        if self._dispose_timer is not None:
            self._dispose_timer()
            self._dispose_timer = None
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

    # ------------------------------------------------------------------
    # watching
    # ------------------------------------------------------------------

    def _base_dir(self) -> str:
        # JS: `resolve(config.base || '.', ctx.baseUrl)`
        base = self.config.get("base") or "."
        if os.path.isabs(base):
            return os.path.abspath(base)
        ctx_base = getattr(self.ctx, "baseUrl", None) or os.getcwd()
        return os.path.abspath(os.path.join(ctx_base, base))

    def _is_ignored(self, path: str) -> bool:
        if os.path.basename(path).startswith("."):
            return True
        rel = os.path.relpath(path, self._base_dir())
        parts = rel.split(os.sep)
        if any(part in ("node_modules", "cache", "data") for part in parts):
            return True
        patterns = self.config.get("ignored") or list(_DEFAULT_IGNORED)
        for pattern in patterns:
            normalized = pattern.lstrip("/")
            if fnmatch.fnmatch(rel, normalized) or fnmatch.fnmatch(rel, normalized.rstrip("/") + "/*"):
                return True
        return False

    def _start_observer(self):
        base = self._base_dir()
        handler = _WatchHandler(self, self._is_ignored)
        self._observer = Observer()
        roots = self.config.get("root") or ["."]
        for root in roots:
            path = root if os.path.isabs(root) else os.path.join(base, root)
            if not os.path.isdir(path):
                self.ctx.logger.warn("hmr: watch root does not exist: %s", path)
                continue
            self._observer.schedule(handler, path, recursive=True)
        self._observer.start()
        self.ctx.logger.info("watching %o in %s", roots, base)

    def _get_mtime(self, name: str):
        try:
            module = importlib.import_module(name)
        except ImportError:
            return None
        filename = getattr(module, "__file__", None)
        if not filename:
            return None
        try:
            return os.path.getmtime(filename)
        except OSError:
            return None

    def watch(self):
        for entry in self.ctx.loader.entries():
            name = entry.options.get("name")
            if not name or name.startswith(("cordis:", "@")):
                continue
            self._snapshots[name] = self._get_mtime(name)

    # ------------------------------------------------------------------
    # event-driven path
    # ------------------------------------------------------------------

    def _on_file_event(self, path: str):
        if self._stopped:
            return
        if self._debounced is None:
            self._debounced = self.ctx.debounce(self._flush, self.config.get("debounce", 100))
        self._stashed.add(os.path.realpath(path))
        self._debounced()

    def _flush(self):
        stashed, self._stashed = self._stashed, set()
        if not stashed:
            return
        self.watch()
        names = self._match(stashed)
        if names:
            self.reload(names)
        self.ctx.emit("hmr/change", names)

    def _match(self, paths: set) -> list:
        matched = []
        for entry in self.ctx.loader.entries():
            name = entry.options.get("name")
            if not name or name.startswith(("cordis:", "@")):
                continue
            try:
                module = importlib.import_module(name)
            except ImportError:
                continue
            filename = getattr(module, "__file__", None)
            if not filename:
                continue
            real = os.path.realpath(filename)
            if any(real == os.path.realpath(path) for path in paths):
                matched.append(name)
        return matched

    # ------------------------------------------------------------------
    # polling fallback
    # ------------------------------------------------------------------

    def _poll(self):
        # watch dynamically — entries may load after the HMR service starts
        for entry in self.ctx.loader.entries():
            name = entry.options.get("name")
            if not name or name.startswith(("cordis:", "@")):
                continue
            if name not in self._snapshots:
                self._snapshots[name] = self._get_mtime(name)
        changed = []
        for name, mtime in list(self._snapshots.items()):
            current = self._get_mtime(name)
            if current is not None and current != mtime:
                changed.append(name)
                self._snapshots[name] = current
        if changed:
            self.reload(changed)
        self.ctx.emit("hmr/change", changed)

    # ------------------------------------------------------------------
    # reloading
    # ------------------------------------------------------------------

    def reload(self, names: list):
        loader = self.ctx.loader
        for entry in loader.entries():
            name = entry.options.get("name")
            if name not in names or entry.fiber is None:
                continue
            try:
                module = importlib.import_module(name)
                importlib.reload(module)
                plugin = loader.unwrap_exports(module)
                old_fiber = entry.fiber
                self.ctx.registry.delete(old_fiber.runtime.callback)
                fiber = self.ctx.registry.plugin(plugin, old_fiber.config)
                fiber.entry = entry
                entry.fiber = fiber
                self.ctx.logger.info("reload plugin at %C", name)
            except BaseException as error:
                self.ctx.logger.warn(error)
