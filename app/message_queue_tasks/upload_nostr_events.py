import asyncio
from datetime import timedelta
from app.core.database import db_session
from app.core.loggr import loggr
from app.db_models import BrainstormRequestStatus
from app.models.grapeRankResult import GrapeRankResult
from app.repos.brainstorm_nsec import (
    get_or_create_brainstorm_observer_nsec_by_pubkey_on_db,
)
from app.repos.brainstorm_request_repo import (
    select_brainstorm_request_by_id_on_db,
    update_brainstorm_request_status_by_id_on_db,
    update_brainstorm_request_ta_status_by_id_on_db,
)
from nostr_sdk import (  # type: ignore
    Client,
    Event,
    EventBuilder,
    Keys,
    Kind,
    NostrSigner,
    Tag,
)
import time
from app.core.config import settings

logger = loggr.get_logger(__name__)

RELAYS: list[str] = [
    x
    for x in [
        settings.nostr_upload_ta_events_relay,
        # settings.nostr_transfer_to_relay2,
    ]
    if x
]


async def init_nostr_client(secret_key_nsec: str) -> Client:
    logger.info("Starting Nostr client...")
    keys: Keys = Keys.parse(secret_key=secret_key_nsec)
    signer: NostrSigner = NostrSigner.keys(keys=keys)
    client = Client(signer=signer)
    relay_count: int = 0
    for relay in RELAYS:
        logger.info(f"Adding relay {relay}")
        try:
            await client.add_relay(relay)
            relay_count += 1
        except:
            logger.error(f"Bad relay {relay}")
    if relay_count == 0:
        raise Exception("No good relay available, shutting down!")

    logger.info("Finished adding relays!")
    result = await client.try_connect(timedelta(seconds=10))
    assert not bool(result.failed)
    logger.info("Nostr Client Connected!!!")

    return client


async def get_events_from_graperank_result(
    grape_rank_result: GrapeRankResult, nostr_client: Client
) -> list[Event]:

    events: list[Event] = []
    logger.info(f"{bool(grape_rank_result.scorecards)}")
    assert grape_rank_result.scorecards is not None
    start_time_sort = time.time()
    logger.info("sorting scorecards...")
    sorted_scorecards = sorted(
        grape_rank_result.scorecards.values(),
        key=lambda sc: sc.influence,
        reverse=True,
    )
    end_time_sort = time.time() - start_time_sort
    logger.info(f"sorted scorecards! took {round(end_time_sort,2)}s")

    for scorecard in sorted_scorecards:

        if scorecard.influence < settings.cutoff_of_valid_graperank_scores:
            continue

        d_tag = scorecard.observee

        rank_tag = round(scorecard.influence * 100)

        trusted_followers_count = scorecard.trusted_followers

        tags = [
            Tag.parse(["d", d_tag]),
            Tag.parse(["rank", str(rank_tag)]),
            Tag.parse(["followers", str(trusted_followers_count)]),
        ]

        event_builder = EventBuilder(
            kind=Kind(30382),
            content="",
        )

        event_builder = event_builder.tags(tags)

        signed_event = await nostr_client.sign_event_builder(event_builder)

        events.append(signed_event)

    return events


async def process_nostr_upload_message(message: dict):
    private_id = message["private_id"]

    grape_rank_result = GrapeRankResult.model_validate(message["result"])

    # Empty scorecards = the graperank run produced no usable output
    # (Java sets success=false when relevantUsers.size() <= 1). Mirror
    # the upstream FAILURE so reports don't count this as a successful
    # TA publication. The important thing is to reach a TERMINAL state
    # so the row doesn't get stuck at WAITING forever.
    if not grape_rank_result.scorecards:
        logger.info(
            f"[nostr upload] no scorecards for private_id={private_id}, "
            f"marking ta_status FAILURE (nothing to publish)"
        )
        async with db_session() as db:
            await update_brainstorm_request_ta_status_by_id_on_db(
                db,
                brainstorm_request_id=private_id,
                status=BrainstormRequestStatus.FAILURE,
            )
            await db.commit()
        return

    observer = next(iter(grape_rank_result.scorecards.values())).observer
    # TODO: generate a new nsec for the observer of the observer
    async with db_session() as db:
        nsec_db_obj, _was_created_now = (
            await get_or_create_brainstorm_observer_nsec_by_pubkey_on_db(
                db, pubkey=observer
            )
        )
        assert nsec_db_obj.pubkey == observer
        await update_brainstorm_request_ta_status_by_id_on_db(
            db,
            brainstorm_request_id=private_id,
            status=BrainstormRequestStatus.ONGOING,
        )

        await db.commit()

    try:
        nostr_client: Client = await init_nostr_client(nsec_db_obj.nsec)

        nostr_events = await get_events_from_graperank_result(
            grape_rank_result, nostr_client
        )

        start_time = time.time()

        # Publish events in parallel batches
        success_count, failure_count = await publish_events_in_batches(
            nostr_client, nostr_events, settings.ta_publish_batch_size
        )
        
        total_events = len(nostr_events)
        success_rate = (success_count / total_events * 100) if total_events > 0 else 0
        
        logger.info(
            f"Published {success_count}/{total_events} events successfully "
            f"({success_rate:.1f}%) for private_id={private_id}"
        )
        
        # Mark as FAILURE if more than 50% failed
        final_status = BrainstormRequestStatus.SUCCESS
        if failure_count > success_count:
            final_status = BrainstormRequestStatus.FAILURE
            logger.error(
                f"Marking private_id={private_id} as FAILURE: "
                f"{failure_count} failures > {success_count} successes"
            )
        
        async with db_session() as db:
            await update_brainstorm_request_ta_status_by_id_on_db(
                db,
                brainstorm_request_id=private_id,
                status=final_status,
            )
            await db.commit()

        final_time = round(time.time() - start_time)
        logger.info(
            f"Took {final_time} seconds to process {total_events} nostr events for private_id={private_id}"
        )
        if nostr_events:
            logger.info(f"Check Nostr Event {nostr_events[0].as_json()}")
    except Exception:
        logger.exception(
            f"[nostr upload] failed for private_id={private_id}, marking ta_status FAILURE"
        )
        async with db_session() as db:
            await update_brainstorm_request_ta_status_by_id_on_db(
                db,
                brainstorm_request_id=private_id,
                status=BrainstormRequestStatus.FAILURE,
            )
            await db.commit()


async def send_nostr_event_with_limit(
    nostr_client: Client, nostr_event: Event, index: int
) -> tuple[bool, str | None]:
    """
    Send a single nostr event and return (success, error_message).
    """
    try:
        sent_event_output = await nostr_client.send_event(nostr_event)
        if sent_event_output.failed:
            error_msg = str(sent_event_output.failed)
            logger.error(f"Failed to publish event {index}: {error_msg}")
            return (False, error_msg)
        elif not sent_event_output.success:
            logger.error(f"Event {index} did not succeed (no OK message)")
            return (False, "No success confirmation")
        return (True, None)
    except Exception as e:
        error_msg = str(e)
        logger.exception(f"Exception publishing event {index}: {error_msg}")
        return (False, error_msg)


async def publish_events_in_batches(
    nostr_client: Client, nostr_events: list[Event], batch_size: int
) -> tuple[int, int]:
    """
    Publish events in parallel batches.
    Returns (success_count, failure_count).
    """
    total_events = len(nostr_events)
    success_count = 0
    failure_count = 0
    
    for batch_start in range(0, total_events, batch_size):
        batch_end = min(batch_start + batch_size, total_events)
        batch = nostr_events[batch_start:batch_end]
        
        if batch_start == 0 or batch_start % (batch_size * 10) == 0:
            logger.info(
                f"Publishing batch {batch_start}-{batch_end} of {total_events} events"
            )
        
        # Publish all events in this batch in parallel
        results = await asyncio.gather(*[
            send_nostr_event_with_limit(nostr_client, event, batch_start + i)
            for i, event in enumerate(batch)
        ])
        
        # Count successes and failures
        for success, error in results:
            if success:
                success_count += 1
            else:
                failure_count += 1
    
    return success_count, failure_count
