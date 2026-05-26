from __future__ import annotations

import asyncio
import random

from app.application.interfaces import PaymentGateway
from app.db.models import Payment
from app.domain.enums import PaymentStatus


class SimulatedPaymentGateway(PaymentGateway):
    def __init__(
        self,
        *,
        min_delay_seconds: float,
        max_delay_seconds: float,
        success_rate: float,
        random_source: random.Random | None = None,
    ) -> None:
        self._min_delay_seconds = min_delay_seconds
        self._max_delay_seconds = max_delay_seconds
        self._success_rate = success_rate
        self._random = random_source or random.Random()

    async def process(self, payment: Payment) -> PaymentStatus:
        del payment
        await asyncio.sleep(self._random.uniform(self._min_delay_seconds, self._max_delay_seconds))
        if self._random.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
