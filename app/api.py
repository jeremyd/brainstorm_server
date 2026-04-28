import asyncio
from contextlib import asynccontextmanager
import random
import string
import time

from app.message_queue_tasks.message_queue_consumer import (
    consume_job_started_messages,
    consume_messages,
    consume_nostr_upload_messages,
    consume_neo4j_write_messages,
    consume_strfry_plugin_messages,
    wait_until_graph_db_is_populated,
)
from app.neo4j_db.driver import test_neo4j_driver
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi_pagination import add_pagination

from app.core.config import settings
from app.core.loggr import loggr
from app.core.sql_admin_panel import add_sql_admin_panel
from app.routers.router import router as main_router
from app.utils.constants import DEPLOY_ENVIRONMENT_LOCAL
from app.services.nsec_encryption_service import bootstrap_keys
from app.nostr_event_transferer.nostr_event_transferer import (
    nostr_event_recent_transferer_cronjob,
    nostr_event_transferer,
)
from app.cronjobs.fail_stale_ongoing_brainstorm_requests import (
    fail_stale_ongoing_brainstorm_requests_cronjob,
)

from app.core.admin_whitelist import init_admin_whitelist

logger = loggr.get_logger(__name__)

openapi_url = None
docs_url = None
redoc_url = None
swagger_ui_oauth2_redirect_url = None

if settings.deploy_environment == DEPLOY_ENVIRONMENT_LOCAL:
    openapi_url = "/openapi.json"
    docs_url = "/docs"
    redoc_url = "/redoc"
    swagger_ui_oauth2_redirect_url = "/docs/oauth2-redirect"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await bootstrap_keys()

    # initialize admin whitelist cache and log config
    init_admin_whitelist()

    # test connectivity with Neo4j
    await test_neo4j_driver()

    worker_tasks = []

    if settings.run_workers:
        logger.info("Starting message queue workers...")
        logger.info(f"Worker concurrency: {settings.worker_concurrency}")
        logger.info(f"TA publish batch size: {settings.ta_publish_batch_size}")
        
        consume_strfry_plugin_messages_task = asyncio.create_task(
            consume_strfry_plugin_messages()
        )
        worker_tasks.append(consume_strfry_plugin_messages_task)

        if settings.perform_nostr_full_sync:
            # populate the STRFRY relay
            logger.info(
                "Populating your local Brainstorm Relay. Brainstorm is deactivated until it is finished"
            )
            await nostr_event_transferer()
            logger.info(
                "Finished populating your local Brainstorm Relay!! Populating your Graph DB..."
            )

            await wait_until_graph_db_is_populated()
            logger.info("Finished populating your Graph Database!! Enjoy Brainstorm!!")
        else:
            logger.info(
                "Skipping intial nostr relay full sync... if you want to do it, modify the env variables and restart."
            )
        # start the regular update cronjob task
        # regular_update_task = asyncio.create_task(nostr_event_recent_transferer_cronjob())
        # worker_tasks.append(regular_update_task)

        # Start the listener tasks
        listener_task = asyncio.create_task(consume_messages())
        listener_nostr_upload_task = asyncio.create_task(consume_nostr_upload_messages())
        listener_neo4j_write_task = asyncio.create_task(consume_neo4j_write_messages())
        listener_ongoing_job_task = asyncio.create_task(consume_job_started_messages())
        fail_stale_ongoing_task = asyncio.create_task(
            fail_stale_ongoing_brainstorm_requests_cronjob()
        )
        
        worker_tasks.extend([
            listener_task,
            listener_nostr_upload_task,
            listener_neo4j_write_task,
            listener_ongoing_job_task,
            fail_stale_ongoing_task,
        ])
    else:
        logger.info("Workers disabled (RUN_WORKERS=false) - running in API-only mode")

    try:
        yield
    finally:
        # Graceful shutdown
        logger.info("Shutting down workers...")
        for task in worker_tasks:
            task.cancel()
        if worker_tasks:
            await asyncio.gather(*worker_tasks, return_exceptions=True)


if settings.run_api_server:
    logger.info("Starting API server...")
    app = FastAPI(
        title="brainstorm_api",
        description="",
        version="0.1.0",
        openapi_url=openapi_url,
        docs_url=docs_url,
        redoc_url=redoc_url,
        swagger_ui_oauth2_redirect_url=swagger_ui_oauth2_redirect_url,
        lifespan=lifespan,
    )
else:
    logger.info("API server disabled (RUN_API_SERVER=false) - running in worker-only mode")
    # Create a minimal app that only runs the lifespan for workers
    app = FastAPI(
        title="brainstorm_worker",
        description="Worker-only mode",
        version="0.1.0",
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )

# Only configure API-specific middleware and routes if running API server
if settings.run_api_server:
    origins = ["*"]
    if settings.deploy_environment != "LOCAL":
        logger.info("Setting specific CORS origin...")
        origins = [settings.frontend_url]

    logger.info("Allowing CORS...")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False,  # TODO: REMOVE THIS ONCE NEEDED
        allow_methods=["*"],
        allow_headers=["*"],
    )


    @app.middleware(middleware_type="http")
    async def log_requests(request: Request, call_next):
        idem = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        logger.info(f"rid={idem} start request path={request.url.path}")
        start_time = time.time()

        response = await call_next(request)

        process_time = (time.time() - start_time) * 1000
        formatted_process_time = "{0:.2f}".format(process_time)
        logger.info(
            f"rid={idem} completed_in="
            f"{formatted_process_time}ms status_code={response.status_code}"
        )
        return response


    @app.get(path="/health")
    async def health_endpoint() -> int:
        return 1


    app.include_router(
        router=main_router,
        prefix="",
    )

    if settings.deploy_environment == "LOCAL":
        add_sql_admin_panel(app)

    add_pagination(app)
else:
    # Worker-only mode: just add a basic health endpoint
    @app.get(path="/health")
    async def health_endpoint() -> int:
        return 1
