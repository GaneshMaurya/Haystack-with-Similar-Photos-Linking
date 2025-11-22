"""
Haystack-style Directory FastAPI app.

Directory instances:
- Share the same DB
- One instance is primary (leader)
- Others are replicas (followers)
- Only primary handles writes
- Replicas redirect writes to primary
- Reads may be served by any instance
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
# import services.directory.models as models
import models
import os
import uvicorn

app = FastAPI()

# Initialize DB on startup
models.init_db()

# NEW: Directory mode (primary or replica)
DIRECTORY_MODE = os.environ.get("DIRECTORY_MODE", "primary")  # "primary" or "replica"
PRIMARY_DIRECTORY_URL = os.environ.get("PRIMARY_DIRECTORY_URL", "http://localhost:8001")

print(f"[Directory] Starting in mode: {DIRECTORY_MODE}")
print(f"[Directory] Primary URL is: {PRIMARY_DIRECTORY_URL}")

# -----------------------
# Request Models
# -----------------------

class AllocateReq(BaseModel):
    size: int
    alt_key: str | None = "orig"


# We remove primary_store (Haystack uploads to ALL replicas)
class AllocateResp(BaseModel):
    photo_id: int
    logical_volume: str
    cookie: str
    replicas: list[str]


class CommitReq(BaseModel):
    photo_id: int


class DeleteReq(BaseModel):
    photo_id: int


# -----------------------
# Helper: Redirect if not primary
# -----------------------

def ensure_primary():
    if DIRECTORY_MODE != "primary":
        # Redirect this write request to the real primary
        raise HTTPException(
            status_code=307,
            detail=f"Redirect to primary",
            headers={"Location": PRIMARY_DIRECTORY_URL}
        )


# -----------------------
# WRITE Endpoints (Primary Only)
# -----------------------

@app.post("/allocate_write", response_model=AllocateResp)
def allocate(req: AllocateReq):
    # If replica, redirect
    ensure_primary()

    try:
        r = models.allocate_write(req.size, req.alt_key or "orig")
        # Must return: photo_id, logical_volume, cookie, replicas[]
        return {
            "photo_id": r["photo_id"],
            "logical_volume": r["logical_volume"],
            "cookie": r["cookie"],
            "replicas": r["replicas"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/commit_write")
def commit(req: CommitReq):
    ensure_primary()

    try:
        return models.commit_write(req.photo_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/delete")
def delete(req: DeleteReq):
    ensure_primary()

    try:
        return models.mark_deleted(req.photo_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------
# READ Endpoints (Primary or Replica)
# -----------------------

@app.get("/photo/{photo_id}")
def get_photo(photo_id: int):
    """Followers can serve this normally."""
    p = models.get_photo(photo_id)
    if not p:
        raise HTTPException(status_code=404, detail="photo not found")
    return p


@app.get("/photos")
def list_photos():
    return models.list_all_photos()


@app.get("/health")
def health():
    db_path = os.environ.get("DB_PATH", "data/directory.db")
    return {
        "service": "directory",
        "mode": DIRECTORY_MODE,
        "db_path": db_path,
        "db_exists": os.path.exists(db_path),
        "primary_directory": PRIMARY_DIRECTORY_URL
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8001")))
