"""The outer stack an effect records for its failure note (inso1337/revl#3390).

Every effect captures the stack it was created on, and the text is read only
when the effect fails. The capture must stay cheap for the effects that do not
fail, and the note must read exactly as it did when the capture was
`traceback.extract_stack()`.
"""

import asyncio
import gc
import linecache
import sys
import traceback
import weakref

import pytest

import cordis.fiber
import cordis.registry
from cordis import Context
from cordis.utils import build_outer_stack


def _reference_outer_stack(offset: int = 0):
    # the capture as it was before #3390, kept verbatim as the oracle
    stack = traceback.extract_stack()[:-1 - offset]
    return lambda: [f"    at {frame.filename}:{frame.lineno} in {frame.name}" for frame in stack]


def _note(error):
    # a stack cut to nothing by sys.tracebacklimit adds no note at all
    return "".join(getattr(error, "__notes__", ["<no note>"]))


def _effect_note(root):
    def execute():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError) as info:
        root.effect(execute)
    return _note(info.value)


async def _plugin_note(root):
    def apply(ctx):
        raise ValueError("bad apply")

    fiber = root.plugin(apply)
    await asyncio.sleep(0.01)
    return _note(fiber._error)


@pytest.fixture(params=[None, 4, 1, 0, -1], ids=lambda v: f"tracebacklimit={v}")
def tracebacklimit(request, monkeypatch):
    if request.param is None:
        monkeypatch.delattr(sys, "tracebacklimit", raising=False)
    else:
        monkeypatch.setattr(sys, "tracebacklimit", request.param, raising=False)
    return request.param


async def test_failure_notes_read_as_they_did_with_extract_stack(tracebacklimit, monkeypatch):
    effect_notes, plugin_notes = [], []
    for capture in (_reference_outer_stack, build_outer_stack):
        monkeypatch.setattr(cordis.fiber, "build_outer_stack", capture)
        monkeypatch.setattr(cordis.registry, "build_outer_stack", capture)
        root = Context()
        # one call site for both captures, so both see the same frames
        effect_notes.append(_effect_note(root)), plugin_notes.append(await _plugin_note(root))
    assert effect_notes[0] == effect_notes[1]
    assert plugin_notes[0] == plugin_notes[1]
    if tracebacklimit is None:
        assert effect_notes[1].startswith("\nouter stack:\n    at ")
        assert "in _effect_note\n" in effect_notes[1]
        assert "in _plugin_note" in plugin_notes[1]


@pytest.mark.parametrize("offset", [0, 1, 3, -1, 10_000])
def test_direct_capture_matches_extract_stack(tracebacklimit, offset):
    got = []
    for capture in (_reference_outer_stack, build_outer_stack):
        got.append(capture(offset)())
    assert got[0] == got[1]


class _Counter:
    def __init__(self, monkeypatch, owner, name):
        self.calls = 0
        original = getattr(owner, name)

        def counted(*args, **kwargs):
            self.calls += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(owner, name, counted)


def test_an_effect_that_does_not_fail_formats_nothing(monkeypatch):
    watched = [
        (traceback, "extract_stack"),
        (traceback, "walk_stack"),
        (traceback.StackSummary, "extract"),
        (traceback.StackSummary, "_extract_from_extended_frame_gen"),
        (linecache, "checkcache"),
        (linecache, "lazycache"),
        (linecache, "getline"),
    ]
    counters = {f"{getattr(o, '__name__', o)}.{n}": _Counter(monkeypatch, o, n) for o, n in watched}
    getter_calls = []
    real = cordis.fiber.build_outer_stack

    def counted_capture(*args):
        getter = real(*args)

        def counted_getter():
            getter_calls.append(1)
            return getter()

        return counted_getter

    monkeypatch.setattr(cordis.fiber, "build_outer_stack", counted_capture)
    root = Context()
    disposers = [root.effect(lambda: (lambda: None)) for _ in range(5)]
    for dispose in disposers:
        dispose()
    assert {name: c.calls for name, c in counters.items() if c.calls} == {}
    assert getter_calls == []


def test_the_capture_keeps_no_frame_alive():
    class Big:
        pass

    def make():
        local = Big()
        return weakref.ref(local), build_outer_stack()

    ref, getter = make()
    gc.collect()
    assert ref() is None
    assert getter()[-1].endswith(" in make")
