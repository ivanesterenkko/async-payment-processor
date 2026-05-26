from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_app_settings, get_db_session, get_idempotency_key
from app.application.payments import PaymentService, to_payment_detail_response
from app.core.config import Settings
from app.core.errors import NotFoundError
from app.schemas.payments import (
    ErrorResponse,
    PaymentAcceptedResponse,
    PaymentCreateRequest,
    PaymentDetailResponse,
)

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentAcceptedResponse,
    responses={400: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment(
    payload: PaymentCreateRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    idempotency_key: Annotated[str, Depends(get_idempotency_key)],
    settings: Annotated[Settings, Depends(get_app_settings)],
) -> PaymentAcceptedResponse:
    service = PaymentService(session, main_routing_key=settings.rabbitmq_main_queue)
    result = await service.create_payment(payload=payload, idempotency_key=idempotency_key)

    return PaymentAcceptedResponse(
        payment_id=result.payment.id,
        status=result.payment.status,
        created_at=result.payment.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentDetailResponse,
    responses={404: {"model": ErrorResponse}},
    status_code=status.HTTP_200_OK,
)
async def get_payment(
    payment_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> PaymentDetailResponse:
    service = PaymentService(session)
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise NotFoundError(
            "Payment not found.",
            code="payment_not_found",
            details={"payment_id": str(payment_id)},
        )

    return to_payment_detail_response(payment)
