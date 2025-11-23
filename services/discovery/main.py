import os
import json
import time
import sys
import uvicorn
import logging
import operator
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, List, Optional

# ============================================
# LOGGING CONFIGURATION (User's Detailed Setup)
# ============================================

LOG_DIR = os.environ.get("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)
DISCOVERY_LOG_FILE = os.path.join(LOG_DIR, "discovery.log")

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Remove any existing handlers to avoid duplicates
for h in root_logger.handlers[:]:
    root_logger.removeHandler(h)

# File handler
file_handler = logging.FileHandler(DISCOVERY_LOG_FILE)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('[DISCOVERY] %(asctime)s - %(levelname)s - %(name)s - %(message)s')
file_handler.setFormatter(file_formatter)
root_logger.addHandler(file_handler)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('[DISCOVERY] %(asctime)s - %(levelname)s - %(name)s - %(message)s')
console_handler.setFormatter(console_formatter)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)
logger.info("=" * 60)
logger.info("Discovery Service starting")
logger.info(f"Log file: {DISCOVERY_LOG_FILE}")
logger.info("=" * 60)


# --- Configuration and Persistence ---
DATA_FILE = os.environ.get("DISCOVERY_FILE", "data/discovery.json")
TTL_SEC = int(os.environ.get("DISCOVERY_TTL_SEC", 15))  # How long a registration is valid

# --- Pydantic Models ---
class ServiceRegistration(BaseModel):
    """Model for a Directory instance registering itself."""
    url: str
    mode: str
    health: str = 'ok'

class ServiceStatus(BaseModel):
    """The internal model stored in discovery.json."""
    url: str
    mode: str
    last_seen: float
    health: str
    service_name: str = Field(None) # Added for deterministic sorting

# --- In-Memory Registry ---
# registry: {service_name (e.g., 'directory_primary') : ServiceStatus}
registry: Dict[str, ServiceStatus] = {}

# --- FastAPI Initialization ---
app = FastAPI(title="Custom Discovery Service")


# --------------------------------------------
# Persistence Helpers
# --------------------------------------------

def load_registry():
    """Loads the registry state from the persistent JSON file."""
    os.makedirs(os.path.dirname(DATA_FILE) or '.', exist_ok=True)
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)
                global registry
                
                # Handling the required 'service_name' for sorting
                def prepare_status(k, v):
                    if 'service_name' not in v:
                        v['service_name'] = k
                    return ServiceStatus(**v)

                registry = {k: prepare_status(k, v) for k, v in data.items()}
            logger.info(f"Loaded {len(registry)} entries from {DATA_FILE}")
        except Exception as e:
            logger.error(f"Failed to load discovery data: {e}", exc_info=True)

def persist_registry():
    """Writes the registry state to the persistent JSON file."""
    try:
        data = {k: v.model_dump() for k, v in registry.items()}
        os.makedirs(os.path.dirname(DATA_FILE) or '.', exist_ok=True)
        with open(DATA_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        logger.debug(f"Persisted {len(registry)} registry entries to {DATA_FILE}")
    except Exception as e:
        logger.error(f"Failed to persist discovery data: {e}", exc_info=True)


# --------------------------------------------
# Registry Management and Election Logic
# --------------------------------------------

def prune_and_get_healthy() -> List[ServiceStatus]:
    """Filters the registry for instances that are healthy and not expired."""
    current_time = time.time()

    # Prune expired services first
    expired_keys = [
        name for name, status in registry.items()
        if current_time - status.last_seen > TTL_SEC
    ]
    for key in expired_keys:
        logger.warning(f"Pruning expired service: {key} (Last seen: {registry[key].last_seen:.0f})")
        del registry[key]

    # Return healthy instances
    healthy = [status for status in registry.values() if status.health == 'ok']
    logger.debug(f"Healthy instances: {[s.service_name for s in healthy]}")
    return healthy

def elect_new_leader(healthy_instances: List[ServiceStatus]) -> Optional[ServiceStatus]:
    """
    Deterministically elects a new leader based on service name (lexicographical order).
    The lowest name wins (e.g., directory_primary < directory_replica).
    """
    if not healthy_instances:
        return None
        
    # Find the instance with the lowest service_name using operator.attrgetter
    new_leader = min(healthy_instances, key=operator.attrgetter('service_name'))
    
    # Update the record in the registry to reflect the new primary status
    # This is CRITICAL: it locks the new leader in the DS registry and triggers
    # the Directory instance's self-promotion on its next heartbeat.
    registry[new_leader.service_name].mode = 'primary'
    
    logger.critical(f"--- LEADER ELECTION --- Declared {new_leader.service_name} as new PRIMARY.")
    persist_registry()
    
    return new_leader


def determine_current_leader() -> ServiceStatus | None:
    """
    Finds the current primary. If not found, triggers a deterministic election.
    """
    
    healthy_instances = prune_and_get_healthy()
    
    # 1. Find the current Primary based on the DS's record
    current_primary = next((s for s in healthy_instances if s.mode == 'primary'), None)
    
    if current_primary:
        return current_primary
    
    # 2. If no Primary is found (it failed/expired), run election
    return elect_new_leader(healthy_instances)


# --------------------------------------------
# API Endpoints
# --------------------------------------------

@app.on_event("startup")
def startup():
    load_registry()
    logger.info("Discovery startup complete")

@app.on_event("shutdown")
def shutdown():
    persist_registry()
    logger.info("Discovery shutdown complete")

@app.post("/register/{service_name}")
def register_service(service_name: str, req: ServiceRegistration):
    """Endpoint for Directory instances to report their status and role."""
    
    current_status = registry.get(service_name)
    registry_mode = req.mode

    # Preservation Logic: If the registry already declared this instance as Primary,
    # preserve that mode regardless of the mode sent in the heartbeat request.
    if current_status and current_status.mode == 'primary':
        registry_mode = 'primary'

    # Update the internal registry
    registry[service_name] = ServiceStatus(
        url=req.url,
        mode=registry_mode,
        health=req.health,
        last_seen=time.time(),
        service_name=service_name
    )

    # Note: Persistence is handled within the leader election flow.
    # We don't persist here to avoid excessive disk I/O on every heartbeat.
    
    # Re-run leader determination immediately after registration
    # This allows for immediate election if the Primary died just before this registration.
    determine_current_leader()

    logger.info(f"Registered/Updated service: {service_name} (Mode: {registry[service_name].mode}, url={req.url})")
    return {"status": "registered", "service_name": service_name}


@app.get("/leader")
def get_leader():
    """
    Returns the URL of the current Primary Directory instance, triggering an
    election if necessary.
    """
    leader = determine_current_leader()

    if leader:
        # If the leader found or elected is a replica that was just promoted, 
        # return its updated primary status to the client.
        return {"url": leader.url, 
                "service_name": leader.service_name, 
                "mode": leader.mode}
        
    logger.error("No healthy Directory instances available for leader selection")
    raise HTTPException(status_code=503, detail="No healthy Directory instances available. Writes halted.")


@app.get("/replicas")
def get_replicas():
    """Returns a list of URLs for all healthy Directory instances for read load balancing."""
    healthy_instances = prune_and_get_healthy()

    if not healthy_instances:
        logger.error("No healthy Directory instances available for reads")
        raise HTTPException(status_code=503, detail="No healthy Directory instances available for reads.")

    urls = [s.url for s in healthy_instances]
    logger.debug(f"Replicas returned: {urls}")
    return {"urls": urls}


@app.get("/health")
def health_check():
    logger.debug("Health check requested")
    return {"status": "ok", "entries": len(registry)}

# Add standard Python run block (usually handled by Docker CMD)
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8501"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_config=None)