"""
Haystack-style Directory FastAPI app.

DIRECTORY MODE LOGIC:
- Uses Discovery Service to manage its mode (primary/replica).
- Periodically registers its status.

REPLICATION LOGIC (ASYNC):
- Primary (only) publishes all DB changes to RabbitMQ for Replicas to consume.
"""

import os
import sys
import time
import asyncio
import httpx
import logging
import uvicorn
from typing import Optional
import pika # NEW: Import pika for RabbitMQ
import json   # REQUIRED for message payload
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
import services.directory.models as models
from typing import List, Dict, Any

# ============================================
# LOGGING CONFIGURATION (User's Detailed Setup - RETAINED)
# ============================================

LOG_DIR = os.environ.get("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)
DIRECTORY_LOG_FILE = os.path.join(LOG_DIR, "directory.log")

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicates
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Add file handler
file_handler = logging.FileHandler(DIRECTORY_LOG_FILE)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s - %(message)s')
file_handler.setFormatter(file_formatter)
root_logger.addHandler(file_handler)

# Add console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s - %(message)s')
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)

# Get logger for this module
logger = logging.getLogger(__name__)

logger.info("=" * 60)
logger.info("Directory Service Starting")
logger.info(f"Log file: {DIRECTORY_LOG_FILE}")
logger.info("=" * 60)

# ============================================
# FASTAPI APP INITIALIZATION
# ============================================

app = FastAPI(title="Directory Service")

# Initialize DB on startup
logger.info("Initializing database...")
try:
    models.init_db()
    logger.info("Database initialization successful")
except Exception as e:
    logger.error(f"Database initialization failed: {e}", exc_info=True)
    raise

# ============================================
# ENVIRONMENT VARIABLES & RABBITMQ SETUP
# ============================================

# NOTE: DIRECTORY_MODE will be dynamically managed by the discovery loop
DIRECTORY_MODE = os.environ.get("DIRECTORY_MODE", "primary") 
DISCOVERY_SERVICE_URL = os.environ.get("DISCOVERY_SERVICE_URL", "http://discovery:8501")
SERVICE_NAME = os.environ.get("HOSTNAME", f"directory_{DIRECTORY_MODE}")
AVAILABLE_STORES_RAW = os.environ.get("AVAILABLE_STORES", "store1,store2")
AVAILABLE_STORES = [s.strip() for s in AVAILABLE_STORES_RAW.split(',') if s.strip()]
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/") # NEW

REGISTRATION_INTERVAL = int(os.environ.get("REGISTRATION_INTERVAL", "5"))
INITIAL_VOLUME_ID = "lv-1"

logger.info(f"Service Name: {SERVICE_NAME}")
logger.info(f"Mode: {DIRECTORY_MODE}")
logger.info(f"Available Stores for Volume: {AVAILABLE_STORES}")
logger.info(f"RabbitMQ URL: {RABBITMQ_URL}")

# Global RabbitMQ connection (used by publisher)
RABBITMQ_CONNECTION: Optional[pika.BlockingConnection] = None
RABBITMQ_CHANNEL: Optional[pika.channel.Channel] = None


# --------------------------------------------
# RabbitMQ Publisher Logic (Primary Only)
# --------------------------------------------

def setup_rabbitmq_publisher():
    """Initializes a blocking connection and channel for publishing events."""
    global RABBITMQ_CONNECTION, RABBITMQ_CHANNEL
    try:
        if RABBITMQ_CONNECTION and RABBITMQ_CONNECTION.is_open:
            return

        params = pika.URLParameters(RABBITMQ_URL)
        RABBITMQ_CONNECTION = pika.BlockingConnection(params)
        RABBITMQ_CHANNEL = RABBITMQ_CONNECTION.channel()
        
        # Declare the exchange used for directory updates (durable topic exchange)
        RABBITMQ_CHANNEL.exchange_declare(
            exchange='directory.updates', 
            exchange_type='topic', 
            durable=True
        )
        logger.info("RabbitMQ publisher connection established.")
    except Exception as e:
        logger.error(f"Failed to set up RabbitMQ publisher: {e}", exc_info=True)
        RABBITMQ_CONNECTION = None
        RABBITMQ_CHANNEL = None


def close_rabbitmq_publisher():
    """Closes the RabbitMQ connection."""
    global RABBITMQ_CONNECTION, RABBITMQ_CHANNEL
    if RABBITMQ_CONNECTION and RABBITMQ_CONNECTION.is_open:
        RABBITMQ_CONNECTION.close()
        logger.info("RabbitMQ publisher connection closed.")
    RABBITMQ_CONNECTION = None
    RABBITMQ_CHANNEL = None


def publish_directory_update(operation: str, payload: Dict[str, Any]):
    """Publishes a structured update message to the directory.updates exchange."""
    if DIRECTORY_MODE != 'primary':
        return # Only the primary publishes updates

    if not RABBITMQ_CHANNEL:
        logger.warning(f"RabbitMQ channel not available for {operation}. Attempting reconnect.")
        setup_rabbitmq_publisher()
        if not RABBITMQ_CHANNEL:
            logger.error(f"Failed to publish {operation}: RabbitMQ unavailable.")
            return

    try:
        routing_key = f"dir.{operation.lower()}"
        message = {
            "operation": operation,
            "timestamp": time.time(),
            "source": SERVICE_NAME,
            "payload": payload
        }
        
        RABBITMQ_CHANNEL.basic_publish(
            exchange='directory.updates',
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2) # Persistent message
        )
        logger.info(f"Published update: {routing_key} for photo {payload.get('photo_id')}")
        
    except pika.exceptions.AMQPConnectionError:
        logger.error("RabbitMQ connection lost. Reconnecting...")
        close_rabbitmq_publisher()
    except Exception as e:
        logger.error(f"Failed to publish {operation} message: {e}", exc_info=True)


# -----------------------
# Request Models
# -----------------------

class AllocateReq(BaseModel):
    size: int
    alt_key: str | None = "orig"


class AllocateResp(BaseModel):
    photo_id: int
    logical_volume: str
    cookie: str
    replicas: list[str]


class CommitReq(BaseModel):
    photo_id: int


class DeleteReq(BaseModel):
    photo_id: int


# --------------------------------------------
# Initialization and Discovery Logic
# --------------------------------------------

def check_and_initialize_volume():
    """
    On startup, ensure a writable volume exists. 
    This must be run synchronously by ALL instances.
    """
    logger.info(f"Checking for initial volume '{INITIAL_VOLUME_ID}'...")
    
    if not models.volume_exists(INITIAL_VOLUME_ID):
        logger.info(f"Initial volume '{INITIAL_VOLUME_ID}' does not exist. Creating...")
        
        if not AVAILABLE_STORES:
            error_msg = "No AVAILABLE_STORES defined to create initial volume"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        try:
            models.insert_new_volume(INITIAL_VOLUME_ID, AVAILABLE_STORES)
            logger.info(f"Created initial volume '{INITIAL_VOLUME_ID}' with replicas: {AVAILABLE_STORES}")
        except Exception as e:
            if not models.volume_exists(INITIAL_VOLUME_ID):
                logger.error(f"Failed to insert initial volume: {e}", exc_info=True)
                raise
            else:
                logger.info(f"Volume already exists (created by another instance)")
    else:
        logger.info(f"Initial volume '{INITIAL_VOLUME_ID}' already exists")


async def register_and_check_promotion():
    """
    Periodically registers with the Discovery Service and checks if this instance
    has been declared the new leader, updating the global state.
    """
    global DIRECTORY_MODE 
    
    internal_url = f"http://{SERVICE_NAME}:8001"
    
    while True:
        try:
            async with httpx.AsyncClient() as client:
                
                # 1. Registration: Tell the DS our current role and health
                reg_payload = {
                    "url": internal_url,
                    "mode": DIRECTORY_MODE,
                    "health": "ok"
                }
                await client.post(f"{DISCOVERY_SERVICE_URL}/register/{SERVICE_NAME}", 
                                  json=reg_payload, timeout=3.0)
                
                # 2. Promotion Check: Ask the DS who the current leader is
                leader_r = await client.get(f"{DISCOVERY_SERVICE_URL}/leader", timeout=3.0)
                
                if leader_r.status_code == 200:
                    leader_data = leader_r.json()
                    
                    if leader_data.get('service_name') == SERVICE_NAME and DIRECTORY_MODE != 'primary':
                        # CRITICAL: DS has promoted us! Change internal state.
                        DIRECTORY_MODE = "primary"
                        logger.critical(">>> PROMOTION SUCCESSFUL: I AM NOW THE PRIMARY LEADER <<<")
                        setup_rabbitmq_publisher() # NEW: Setup publisher upon taking leadership
                        
                    elif leader_data.get('service_name') != SERVICE_NAME and DIRECTORY_MODE == 'primary':
                        # Fencing/Demotion: Another instance was elected. Step down.
                        DIRECTORY_MODE = "replica"
                        logger.warning("<<< DEMOTION OCCURRED: Another service was elected Primary. I am now a Replica. >>>")
                        close_rabbitmq_publisher() # NEW: Close publisher upon demotion
                        
                    elif leader_data.get('service_name') != SERVICE_NAME:
                         logger.info(f"Primary is {leader_data.get('service_name')}. Running as {DIRECTORY_MODE}.")
                
        except httpx.RequestError as e:
            logger.error(f"Network error during discovery operation: {e}")
        except asyncio.CancelledError:
            logger.info("Discovery registration task cancelled")
            break
        except Exception as e:
            logger.exception(f"Unexpected error during discovery loop: {e}")
        
        try:
            await asyncio.sleep(REGISTRATION_INTERVAL)
        except asyncio.CancelledError:
            break

# --------------------------------------------
# Initial Sync and Consumption Logic (To be built later for Replicas)
# --------------------------------------------


# -----------------------
# Startup & Shutdown Hooks
# -----------------------

@app.on_event("startup")
async def startup_event():
    logger.info("Startup event triggered")
    
    # 1. Ensure the initial logical volume is ready in the shared DB
    try:
        logger.info("Initializing volumes...")
        check_and_initialize_volume()
        logger.info("Volume initialization successful")
    except RuntimeError as e:
        logger.error(f"FATAL DIRECTORY ERROR during volume initialization: {e}", exc_info=True)
        raise
    
    # 2. Setup RabbitMQ publisher if we start as primary (Initial state)
    if DIRECTORY_MODE == 'primary':
        setup_rabbitmq_publisher()
    
    # 3. Start the periodic registration and promotion check task
    logger.info(f"Starting periodic discovery registration task for {SERVICE_NAME}...")
    try:
        app.state.discovery_task = asyncio.create_task(register_and_check_promotion())
        logger.info(f"Directory service started successfully in {DIRECTORY_MODE} mode")
    except Exception as e:
        logger.error(f"Failed to start discovery registration task: {e}", exc_info=True)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutdown event triggered")
    
    # 1. Close RabbitMQ publisher connection
    close_rabbitmq_publisher()

    # 2. Cancel the periodic registration task
    if hasattr(app.state, 'discovery_task'):
        logger.info("Cancelling discovery registration task...")
        app.state.discovery_task.cancel()
        try:
            await app.state.discovery_task
        except asyncio.CancelledError:
            logger.info("Discovery registration task cancelled successfully")
    
    logger.info("Directory service shutdown complete")


# -----------------------
# WRITE Endpoints (Primary Only Enforcement)
# -----------------------

def ensure_primary():
    """Ensure this instance is in primary mode for write operations."""
    if DIRECTORY_MODE != "primary":
        logger.warning(f"Write attempt on {DIRECTORY_MODE} instance (only primary accepts writes)")
        raise HTTPException(
            status_code=405,
            detail=f"This directory instance is a '{DIRECTORY_MODE}' and does not accept writes."
        )


@app.post("/allocate_write", response_model=AllocateResp)
def allocate(req: AllocateReq):
    """
    Allocate a new photo ID and return write location.
    Only available on primary instance.
    """
    logger.info(f"[ALLOCATE] size={req.size}, alt_key={req.alt_key}")
    
    try:
        ensure_primary()
        logger.debug(f"[ALLOCATE] Primary mode verified")
        
        # NOTE: Pass AVAILABLE_STORES to models layer for potential LV rollover
        r = models.allocate_write(req.size, req.alt_key or "orig", available_stores=AVAILABLE_STORES)
        
        # --- 1. Update Volume Usage (Simulated Write) ---
        try:
            models.update_volume_usage(r["logical_volume"], req.size)
            logger.info(f"[ALLOCATE] Volume usage updated for {r['logical_volume']}")
        except Exception as update_e:
            logger.error(f"[ALLOCATE] FAILED to update volume usage: {update_e}", exc_info=True)

        # --- 2. Publish Allocation Event ---
        publish_directory_update("ALLOCATE", {
            "photo_id": r["photo_id"],
            "logical_volume": r["logical_volume"],
            "replicas": r["replicas"],
            "cookie": r["cookie"],
            "alt_key": req.alt_key or "orig",
            "size": req.size,
            "status": "alloc"
        })

        logger.info(f"[ALLOCATE] SUCCESS: photo_id={r['photo_id']}, lv={r['logical_volume']}, replicas={r['replicas']}")
        
        return r
        
    except HTTPException as e:
        logger.error(f"[ALLOCATE] HTTP Exception: {e.status_code} - {e.detail}")
        raise
    except RuntimeError as e:
        logger.error(f"[ALLOCATE] Allocation failed: {e}")
        raise HTTPException(status_code=503, detail="No writable volume available for allocation.")
    except Exception as e:
        logger.error(f"[ALLOCATE] Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/commit_write")
def commit(req: CommitReq):
    """
    Commit a write operation (mark photo as active).
    Only available on primary instance.
    """
    logger.info(f"[COMMIT] photo_id={req.photo_id}")
    
    try:
        ensure_primary()
        logger.debug(f"[COMMIT] Primary mode verified")
        
        result = models.commit_write(req.photo_id)
        
        # --- Publish Commit Event ---
        publish_directory_update("COMMIT", {
            "photo_id": req.photo_id,
            "status": "active"
        })
        
        logger.info(f"[COMMIT] SUCCESS: photo_id={req.photo_id}")
        return result
        
    except HTTPException as e:
        logger.error(f"[COMMIT] HTTP Exception: {e.status_code} - {e.detail}")
        raise
    except Exception as e:
        logger.error(f"[COMMIT] Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delete")
def delete(req: DeleteReq):
    """
    Mark a photo as deleted (soft delete).
    Only available on primary instance.
    """
    logger.info(f"[DELETE] photo_id={req.photo_id}")
    
    try:
        ensure_primary()
        logger.debug(f"[DELETE] Primary mode verified")
        
        result = models.mark_deleted(req.photo_id)
        
        # --- Publish Delete Event ---
        publish_directory_update("DELETE", {
            "photo_id": req.photo_id
        })
        
        logger.info(f"[DELETE] SUCCESS: photo_id={req.photo_id}")
        return result
        
    except HTTPException as e:
        logger.error(f"[DELETE] HTTP Exception: {e.status_code} - {e.detail}")
        raise
    except Exception as e:
        logger.error(f"[DELETE] Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------
# READ Endpoints (Primary or Replica)
# -----------------------

@app.get("/photo/{photo_id}")
def get_photo(photo_id: int):
    """
    Retrieve photo metadata.
    Available on both primary and replica instances.
    """
    logger.debug(f"[GET_PHOTO] photo_id={photo_id}")
    
    try:
        p = models.get_photo(photo_id)
        
        if not p:
            logger.warning(f"[GET_PHOTO] photo_id={photo_id} not found")
            raise HTTPException(status_code=404, detail="photo not found")
        
        logger.debug(f"[GET_PHOTO] SUCCESS: photo_id={photo_id}")
        return p
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET_PHOTO] Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/photos")
def list_photos():
    """
    List all photos.
    Available on both primary and replica instances.
    """
    logger.debug("[LIST_PHOTOS] Listing all photos")
    
    try:
        result = models.list_all_photos()
        logger.debug(f"[LIST_PHOTOS] Returned {result.get('total', 0)} photos")
        return result
        
    except Exception as e:
        logger.error(f"[LIST_PHOTOS] Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------
# HEALTH CHECK & STATUS
# -----------------------

@app.get("/health")
def health():
    """
    Health check endpoint.
    Available on both primary and replica instances.
    """
    logger.debug("[HEALTH] Health check requested")
    
    db_path = os.environ.get("DB_PATH", "data/directory.db")
    db_exists = os.path.exists(db_path)
    
    return {
        "service": "directory",
        "mode": DIRECTORY_MODE,
        "db_path": db_path,
        "db_exists": db_exists,
        "timestamp": time.time()
    }


@app.get("/status")
def status():
    """
    Detailed status endpoint.
    Available on both primary and replica instances.
    """
    logger.debug("[STATUS] Status check requested")
    
    try:
        photos = models.list_all_photos()
        
        return {
            "service": "directory",
            "mode": DIRECTORY_MODE,
            "total_photos": photos.get('total', 0),
            "timestamp": time.time(),
            "status": "ok"
        }
    except Exception as e:
        logger.error(f"[STATUS] Error retrieving status: {e}", exc_info=True)
        return {
            "service": "directory",
            "mode": DIRECTORY_MODE,
            "status": "error",
            "error": str(e)
        }


# --------------------------------------------
# EXPORT ENDPOINT (For Replica Bootstrapping)
# --------------------------------------------

@app.get("/sync/full-dump")
def get_full_data_dump():
    """
    Retrieves all photo metadata. Used by new Replicas for bootstrapping history.
    """
    if DIRECTORY_MODE != 'primary':
        raise HTTPException(status_code=405, detail="Only Primary can provide the full data dump.")
        
    try:
        data = models.list_all_photos_data() # New function in models.py
        logger.info(f"Exported {len(data)} photo records for snapshot sync.")
        return data
    except Exception as e:
        logger.error(f"Failed to generate full data dump: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database read error during export.")


# ============================================
# MAIN
# ============================================

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Starting Directory Service with Uvicorn")
    logger.info(f"Host: 0.0.0.0, Port: {os.getenv('PORT', '8001')}")
    logger.info(f"Mode: {DIRECTORY_MODE}")
    logger.info("=" * 60)
    
    # We pass log_config=None because we handle all logging setup above via the root_logger
    port = int(os.getenv("PORT", "8001"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_config=None)