"""Port of packages/loader/tests/index.spec.ts."""

import asyncio

import pytest

from cordis import Context, FiberState

from .conftest import Mock, sleep
from .loader_utils import MockLoader, install


@pytest.fixture()
def basic():
    root = Context()
    loader = MockLoader(root)

    async def setup():
        await root.plugin(loader)

    return root, loader, setup


async def test_loader_basic_support():
    root = Context()
    loader = await install(root)

    foo = loader.mock("foo", lambda ctx, config: ctx.on("internal/update", lambda this, *a: None))
    bar = loader.mock("bar", lambda ctx, config: ctx.on("internal/update", lambda this, *a: None))
    qux = loader.mock("qux", lambda ctx, config: ctx.on("internal/update", lambda this, *a: None))

    # loader initiate
    await loader.read(
        [
            {"id": "1", "name": "foo"},
            {"id": "2", "name": "@cordisjs/plugin-group", "config": [
                {"id": "3", "name": "bar", "config": {"a": 1}},
                {"id": "4", "name": "qux", "disabled": True},
            ]},
        ]
    )

    loader.expect_enable(foo)
    loader.expect_enable(bar)
    loader.expect_disable(qux)
    assert len(foo.calls) == 1
    assert len(bar.calls) == 1
    assert len(qux.calls) == 0

    # loader update
    foo.reset_calls()
    bar.reset_calls()
    await loader.read([{"id": "1", "name": "foo"}, {"id": "4", "name": "qux"}])

    loader.expect_enable(foo)
    loader.expect_disable(bar)
    loader.expect_enable(qux)
    assert len(foo.calls) == 0
    assert len(bar.calls) == 0
    assert len(qux.calls) == 1

    # plugin self-update
    loader.expect_fiber("1").update({"a": 3})
    await sleep()
    assert loader.data == [
        {"id": "1", "name": "foo", "config": {"a": 3}},
        {"id": "4", "name": "qux"},
    ]

    # plugin self-dispose
    loader.expect_fiber("1").dispose()
    await sleep()
    assert loader.data == [
        {"id": "1", "name": "foo", "disabled": True, "config": {"a": 3}},
        {"id": "4", "name": "qux"},
    ]


async def test_loader_intercept_config():
    root = Context()
    loader = await install(root)

    release = asyncio.Future()
    loader.mock("foo", lambda ctx, config: release)
    bar = loader.mock("bar", lambda ctx, config: ctx.on("internal/update", lambda this, *a: True))
    bar.inject = ["never"]
    loader.mock("qux", lambda ctx, config: None)

    # pending
    foo = await loader.create({"name": "foo"})
    bar_id = await loader.create({"name": "bar"})
    qux_id = await loader.create(
        {"name": "qux", "inject": {"loader": True}, "intercept": {"loader": {"await": True}}}
    )

    await sleep()
    assert loader.expect_fiber(foo).state == FiberState.LOADING
    assert loader.expect_fiber(bar_id).state == FiberState.PENDING
    assert loader.expect_fiber(qux_id).state == FiberState.PENDING

    # resolved
    release.set_result(None)
    await sleep()
    assert loader.expect_fiber(foo).state == FiberState.ACTIVE
    assert loader.expect_fiber(bar_id).state == FiberState.PENDING
    assert loader.expect_fiber(qux_id).state == FiberState.ACTIVE
