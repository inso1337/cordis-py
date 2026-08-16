"""Reentrant-disposal and lifecycle-interleaving regression tests.

These scenarios are ported from the lifecycle hardening that DeepSeek
Harness applied to its vendored cordis fiber (deepseek-ai/deepseek-harness,
vendor/cordis — "fiber.ts lifecycle hardening"), which closes reentrancy
gaps in the upstream implementation this package was ported from. Each test
fails against the unhardened fiber.
"""

import asyncio

from cordis import Context, CordisError, FiberState

from .conftest import Mock, event, get_hook_snapshot, sleep


async def test_unload_snapshot_includes_effect_created_during_setup():
    """An unload begun from inside an effect's setup must still collect that
    effect's cleanup (ownership before execution)."""
    root = Context()
    cleaned = []
    holder = {}

    def plugin(ctx, config):
        def arm():
            def setup():
                # tear the fiber down while this setup is still on the stack
                ctx.fiber.dispose()
                return lambda: cleaned.append("inner")

            ctx.fiber.effect(setup)

        holder["arm"] = arm

    await root.plugin(plugin)
    holder["arm"]()
    await sleep()
    assert cleaned == ["inner"]


async def test_unload_joins_inflight_async_cleanup():
    """An owner unloading while one of its effects is already running an
    async cleanup must await that cleanup, not skip it."""
    root = Context()
    seq = []
    gate = asyncio.Event()
    holder = {}

    def plugin(ctx, config):
        def setup():
            def cleanup():
                async def run():
                    seq.append("cleanup-start")
                    await gate.wait()
                    seq.append("cleanup-end")

                return run()

            return cleanup

        holder["inner"] = ctx.fiber.effect(setup)

    fiber = await root.plugin(plugin)
    holder["inner"]()  # start the async cleanup
    disposal = fiber.dispose()
    await sleep()
    assert seq == ["cleanup-start"]
    assert fiber.state == FiberState.UNLOADING  # unload waits on the cleanup
    gate.set()
    await disposal
    await sleep()
    assert seq == ["cleanup-start", "cleanup-end"]
    assert fiber.state == FiberState.DISPOSED


async def test_effect_during_dependency_unload_is_rejected():
    """A cleanup-time effect registration would escape the unload snapshot —
    it must be rejected with INACTIVE_EFFECT."""
    root = Context()
    caught = {}
    leaked = Mock()

    def provider(ctx, config):
        ctx.provide("foo")
        ctx.set("foo", 42)

    def consumer(ctx, config):
        def setup():
            def cleanup():
                try:
                    ctx.fiber.effect(lambda: leaked, "leak")
                except CordisError as error:
                    caught["error"] = error

            return cleanup

        ctx.fiber.effect(setup)

    p = await root.plugin(provider)
    fiber = root.inject(["foo"], consumer)
    await fiber
    await p.dispose()  # withdraw the dependency → consumer unloads
    await sleep()
    assert caught["error"].code == "INACTIVE_EFFECT"
    assert len(leaked.calls) == 0


async def test_disposal_notification_is_contained_per_observer():
    """One throwing `internal/plugin` observer must neither starve its peers
    nor interrupt the ownership cleanup that follows."""
    root = Context()
    seen = []

    def bad(*args):
        fiber = args[-1]
        if fiber.uid is None:
            raise Exception("observer boom")

    def good(*args):
        fiber = args[-1]
        if fiber.uid is None:
            seen.append("good")

    root.on("internal/plugin", bad)
    root.on("internal/plugin", good)

    def plugin(ctx, config):
        return lambda: seen.append("cleanup")

    fiber = await root.plugin(plugin)
    await fiber.dispose()
    await sleep()
    assert "good" in seen
    assert "cleanup" in seen


async def test_successful_reload_clears_previous_failure():
    """A fiber that failed once and later reloads successfully must not
    keep reporting the stale failure."""
    root = Context()
    state = {"fail": True}

    def consumer(ctx, config):
        if state["fail"]:
            raise Exception("boom")

    def provider(ctx, config):
        ctx.provide("foo")
        ctx.set("foo", 1)

    fiber = root.inject(["foo"], consumer)
    await sleep()

    p = await root.plugin(provider)
    await sleep()
    assert fiber.state == FiberState.FAILED

    state["fail"] = False
    await p.dispose()
    await root.plugin(provider)
    await sleep()
    assert fiber.state == FiberState.ACTIVE
    await fiber  # must not raise the stale error


async def test_disposal_before_first_checkpoint_skips_execution():
    """A fiber disposed between activation and the reload task's first
    checkpoint must not execute its plugin at all."""
    root = Context()
    ran = Mock()
    fiber = root.plugin(ran)
    fiber.dispose()
    await sleep()
    assert len(ran.calls) == 0
    assert fiber.state == FiberState.DISPOSED


async def test_dispose_pending_fiber_drains_its_effects():
    """A fiber that never activated still owns effects attached while it was
    pending (e.g. listeners on its context) — disposal must drain them."""
    root = Context()
    handler = Mock()

    def consumer(ctx, config):
        pass

    fiber = root.inject(["foo"], consumer)  # stays pending: no `foo`
    fiber.ctx.on(event, handler)
    assert get_hook_snapshot(root)[event] == 1

    await fiber.dispose()
    await sleep()
    root.emit(event)
    assert len(handler.calls) == 0
    assert get_hook_snapshot(root).get(event) is None
