"""The one synchronisation primitive this package has.

Mojo 1.0 ships no mutex -- there is no `std.sync` -- and no thread API
either, so a caller driving a client from several threads is necessarily
bringing its own, through MAX or through `pthread_create`. `std.atomic` is
the whole toolbox, and `_Latch` is what gets built from it.

`Producer` holds state that a C callback reaches by address -- the
delivery-report failure list. librdkafka runs that callback on **whichever
thread called `poll` / `flush`**, never on a background thread of its own,
so several threads draining one producer are several writers to one list.
That is what this was built for.

`Consumer` uses it for something different: `consume()` must not run twice
at once on one consumer, and the latch is how that is **detected and
refused** rather than serialised. See `try_acquire`.

Two rules keep every use of it correct, and both have a specific failure
mode behind them:

- **No FFI call inside a critical section.** The callback that contends for
  a latch is invoked from inside a librdkafka call, so a caller holding one
  across a crossing into C can be waiting on a thread that is waiting on it.
  Decode, copy, or otherwise finish with C *before* acquiring.
- **Never raise while holding one.** There is no `finally` in play here and
  destructors do not run for a lock that is not a value, so an exception
  escaping a critical section leaves it held -- which does not crash, it
  spins the next acquirer forever. Build the message inside, raise outside.
"""

from std.atomic import Atomic


struct _Latch(Movable):
    """A spinlock over a single `Atomic`.

    A spin is the right shape for the sections this guards: every one of
    them is a handful of instructions on a `List` -- an append, a length, a
    copy, three pointer stores -- with no syscall and no C call inside. It
    would be the wrong shape for anything that blocks, which is exactly why
    the two rules above are rules and not preferences.

    Not `Copyable`: an `Atomic` cannot be copied, and a copied lock would
    protect nothing anyway.
    """

    var _flag: Atomic[DType.int64]

    def __init__(out self):
        self._flag = Atomic[DType.int64](0)

    def try_acquire(mut self) -> Bool:
        """Take the latch if it is free, or report that it is not.

        For the case where contention is a **caller error rather than
        ordinary sharing**: `Consumer.consume()` is undefined behaviour if
        two threads enter it on one consumer, so it refuses the second
        caller instead of queueing it. Serialising there would hide the bug
        and hand back a silently different execution; failing loudly is the
        whole point.
        """
        var expected = Int64(0)
        return self._flag.compare_exchange(expected, 1)

    def acquire(mut self):
        # `compare_exchange` writes the observed value back into `expected`
        # on failure, so it is reset every iteration rather than hoisted out
        # of the loop -- a hoisted one compares against the wrong value from
        # the second attempt on and never succeeds.
        while True:
            var expected = Int64(0)
            if self._flag.compare_exchange(expected, 1):
                return

    def release(mut self):
        self._flag.store(0)
