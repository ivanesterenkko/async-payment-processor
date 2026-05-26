from __future__ import annotations

import asyncio
from pathlib import Path


class Heartbeat:
    def __init__(self, file_path: str) -> None:
        self._file_path = Path(file_path)
        self._task: asyncio.Task[None] | None = None

    async def beat(self) -> None:
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        self._file_path.write_text("ok", encoding="utf-8")

    async def start(self, *, interval_seconds: float) -> None:
        await self.beat()
        self._task = asyncio.create_task(
            self._run(interval_seconds),
            name=f"heartbeat:{self._file_path}",
        )

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self, interval_seconds: float) -> None:
        while True:
            await self.beat()
            await asyncio.sleep(interval_seconds)
