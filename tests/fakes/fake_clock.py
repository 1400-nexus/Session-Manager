class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self._now: float = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds

    async def sleep(self, seconds: float) -> None:
        self.advance(seconds)
