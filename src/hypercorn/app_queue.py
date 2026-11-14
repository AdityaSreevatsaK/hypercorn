from __future__ import annotations

from collections import deque
from typing import Any

from .typing import ASGIReceiveEvent


class AppQueue:
    """Bounded ASGI input queue whose producers can be released on disconnect.

    Condition must be an asyncio.Condition or trio.Condition. In either worker
    the queue has the same backpressure and shutdown semantics.
    """

    def __init__(self, max_size: int, condition: Any) -> None:
        self._messages: deque[ASGIReceiveEvent] = deque()
        self._max_size = max_size
        self._condition = condition
        self._finished = False

    async def __call__(self, message: ASGIReceiveEvent) -> None:
        async with self._condition:
            while not self._finished and 0 < self._max_size <= len(self._messages):
                await self._condition.wait()
            if not self._finished:
                self._messages.append(message)
                self._condition.notify_all()

    async def receive(self) -> ASGIReceiveEvent:
        async with self._condition:
            while not self._finished and not self._messages:
                await self._condition.wait()
            if self._finished:
                return {"type": "http.disconnect"}
            message = self._messages.popleft()
            self._condition.notify_all()
            return message

    async def finish(self) -> None:
        async with self._condition:
            self._finished = True
            self._messages.clear()
            self._condition.notify_all()
