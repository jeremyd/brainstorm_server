from app.core.database import db_session
from app.core.loggr import loggr
from app.db_models import BrainstormRequestStatus, BrainstormRequest

from app.repos.brainstorm_request_repo import (
    update_brainstorm_request_status_by_id_on_db,
)
from sqlalchemy import select

logger = loggr.get_logger(__name__)


async def process_job_started_message(message: dict):
    """Mark a request as ONGOING when the worker picks it up.

    IMPORTANT: only update `status` here. The previous implementation used
    `update_brainstorm_request_result_by_id_on_db`, which also blanked out
    `result` and `count_values`. If a results message ever raced ahead of
    (or arrived alongside) this one, that would clobber a completed SUCCESS
    row back to ONGOING with empty data. Also avoid the downgrade if the
    request has already terminated.
    """
    request_id = message["id"]

    terminal_statuses = {
        BrainstormRequestStatus.SUCCESS.value,
        BrainstormRequestStatus.FAILURE.value,
    }

    async with db_session() as db:
        existing_status = (
            await db.execute(
                select(BrainstormRequest.status).where(
                    BrainstormRequest.private_id == request_id
                )
            )
        ).scalar_one_or_none()

        if existing_status in terminal_statuses:
            logger.info(
                f"[job_started] private_id={request_id} already in terminal "
                f"status={existing_status}; not downgrading to ongoing"
            )
            return

        await update_brainstorm_request_status_by_id_on_db(
            db,
            brainstorm_request_id=request_id,
            status=BrainstormRequestStatus.ONGOING,
        )
        await db.commit()
