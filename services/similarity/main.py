import os
import faiss
import numpy as np
import torch
from PIL import Image
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from torchvision import models
import uvicorn
from typing import List
import logging
from torchvision.transforms import Compose, Resize, CenterCrop, ToTensor, Normalize
import json
import io

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- FastAPI App Initialization ---
app = FastAPI(title="Image Similarity Service")

# --- Global Variables & Constants (Updated Paths) ---
# Base directory is two levels up from services/similarity/main.py
DATA_BASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'data') 

# NOTE: UPLOAD_FOLDER is removed, as files are now processed in memory.
INDEX_FILE = os.path.join(DATA_BASE_DIR, "image_index.faiss")
MAPPING_FILE = os.path.join(DATA_BASE_DIR, "photo_id_to_embedding_id.json") 

# Ensure the data directory exists
os.makedirs(DATA_BASE_DIR, exist_ok=True)
logger.info(f"Data folder '{DATA_BASE_DIR}' is ready. Files will be processed in memory.")

# --- Model Loading ---
logger.info("Loading pre-trained ResNet50 model...")
# Load a pre-trained ResNet50 model and remove the final classification layer
model = models.resnet50(pretrained=True)
model = torch.nn.Sequential(*(list(model.children())[:-1]))
model.eval()  # Set the model to evaluation mode
logger.info("Model loaded successfully.")

# --- Image Preprocessing ---
preprocess = Compose([
    Resize(256),
    CenterCrop(224),
    ToTensor(),
    Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
])

# --- Feature Extraction ---
def get_image_embedding(image_bytes: bytes): # MODIFIED: Accepts bytes instead of path
    """
    Generates a vector embedding for a given image (passed as bytes) and L2-normalizes it.
    """
    logger.debug(f"Generating embedding from image bytes.")
    try:
        # Open image from in-memory stream
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        image_tensor = preprocess(image).unsqueeze(0)
        with torch.no_grad():
            embedding = model(image_tensor)
        embedding_np = embedding.squeeze().numpy()
        
        # L2 Normalization (CRUCIAL for Cosine Similarity with IndexFlatIP)
        norm = np.linalg.norm(embedding_np)
        if norm > 0:
            embedding_np = embedding_np / norm
            
        logger.debug("Embedding generated and normalized.")
        return embedding_np.astype('float32')
    except Exception as e:
        logger.error(f"Failed to generate embedding: {e}")
        raise

# --- FAISS Indexing ---
embedding_dim = 2048  # ResNet50 output feature dimension
index = faiss.IndexFlatIP(embedding_dim) 

# --- Image List Management ---
# image_list is now removed
photo_id_to_embedding_id = {}


# Load existing index and image list if they exist
if os.path.exists(INDEX_FILE):
    logger.info(f"Loading existing FAISS index from '{INDEX_FILE}'...")
    try:
        index = faiss.read_index(INDEX_FILE)
        # Removed image_list loading
        with open(MAPPING_FILE, "r") as f: 
            photo_id_to_embedding_id = json.load(f)
        logger.info(f"Loaded {index.ntotal} vectors and {len(photo_id_to_embedding_id)} image paths. Index type: {type(index).__name__}")
    except Exception as e:
        logger.error(f"Failed to load existing index or mapping: {e}. Starting fresh.")
        index = faiss.IndexFlatIP(embedding_dim)
        photo_id_to_embedding_id = {}
else:
    logger.info("No existing index found. Starting with a new IndexFlatIP.")

# --- API Endpoints ---
@app.post("/upload/")
async def upload_image(file: UploadFile = File(...), photo_id: str = Form(...)):
    """
    Upload an image, generate its normalized embedding in-memory, and add it to the FAISS index.
    """
    logger.info(f"Received upload request for file: '{file.filename}' with photo_id: '{photo_id}'")
    
    # NEW: Read file content directly into memory
    try:
        image_bytes = await file.read()
    except Exception as e:
        logger.error(f"Failed to read uploaded file: {e}")
        raise HTTPException(status_code=500, detail="Could not read uploaded file content.")

    # Generate and add embedding to index
    try:
        embedding = get_image_embedding(image_bytes) # MODIFIED: Pass bytes
        
        # FAISS expects a 2D array: (1, embedding_dim)
        index.add(np.array([embedding]))
        
        # Store the photo_id to embedding_id mapping
        embedding_id = index.ntotal - 1  # FAISS adds to the end, so it's the last index
        photo_id_to_embedding_id[photo_id] = embedding_id
        # Removed image_list append
        logger.info(f"Added new embedding for photo_id '{photo_id}'. Total items in index: {index.ntotal}")
    except Exception as e:
        logger.error(f"Failed during embedding or indexing for '{file.filename}' (photo_id: '{photo_id}'): {e}")
        # No need to clean up file on disk
        raise HTTPException(status_code=500, detail="Failed to process image and update index.")

    # Persist the index and photo_id mapping
    try:
        faiss.write_index(index, INDEX_FILE)
        # Removed image_list writing
        with open(MAPPING_FILE, "w") as f: 
            json.dump(photo_id_to_embedding_id, f)
        logger.info(f"Successfully saved index to '{INDEX_FILE}' and photo_id mapping.")
    except Exception as e:
        logger.error(f"Failed to persist index or photo_id mapping to disk: {e}")
        
    return {"message": f"Image '{file.filename}' (photo_id: '{photo_id}') uploaded and indexed successfully."}

@app.post("/find_similar/")
async def find_similar_images(file: UploadFile = File(...), k: int = Form(5)):
    """
    Find and return the k most similar images (highest Cosine Similarity) to the uploaded image.
    """
    logger.info(f"Received similarity search for '{file.filename}' with k={k}")
    if index.ntotal == 0:
        logger.warning("Similarity search requested, but index is empty.")
        return {"message": "No images have been indexed yet. Please upload images first."}

    # NEW: Read query file content directly into memory
    try:
        query_bytes = await file.read()
    except Exception as e:
        logger.error(f"Failed to read query file: {e}")
        raise HTTPException(status_code=500, detail="Could not read query file content.")
        
    # Generate normalized embedding for the query image
    try:
        query_embedding = get_image_embedding(query_bytes) # MODIFIED: Pass bytes
        logger.info("Generated normalized embedding for query image.")
    except Exception as e:
        logger.error(f"Could not generate embedding for query image: {e}")
        raise HTTPException(status_code=500, detail="Could not generate embedding for query image.")

    # Search for k + 10 neighbors to filter out potential duplicates/self-matches
    search_limit = k + 10 
    logger.info(f"Searching IndexFlatIP for up to {search_limit} nearest neighbors (highest Cosine Similarity)...")
    
    similarities, indices = index.search(np.array([query_embedding]), search_limit)
    logger.info("Search complete.")

    # --- Process and filter results ---
    result_indices = indices[0].tolist() 
    result_similarities = similarities[0].tolist() 
        
    embedding_id_to_photo_id = {v: k for k, v in photo_id_to_embedding_id.items()}
    
    similar_photo_ids = []
    similar_distances = [] 
    
    SIMILARITY_THRESHOLD_FOR_DUPLICATE = 0.999999
    
    for faiss_index_id, sim_val in zip(result_indices, result_similarities):
        # 1. Skip invalid FAISS indices
        if faiss_index_id == -1:
            continue
            
        # 2. Skip duplicates/self-matches (i.e., any image where the similarity is near 1.0)
        if sim_val > SIMILARITY_THRESHOLD_FOR_DUPLICATE:
            continue
            
        # 3. Get the corresponding photo_id
        photo_id = embedding_id_to_photo_id.get(faiss_index_id)
        
        if photo_id:
            similar_photo_ids.append(photo_id)
            similar_distances.append(float(sim_val))
            
        # 4. Stop once we have 'k' unique (non-duplicate) results
        if len(similar_photo_ids) >= k:
            break

    logger.info(f"Found {len(similar_photo_ids)} truly similar images (Cosine Similarity used).")

    return {"similar_photo_ids": similar_photo_ids, "distances": similar_distances}

if __name__ == "__main__":
    logger.info("Starting Image Similarity Service with Uvicorn...")
    uvicorn.run(app, host="0.0.0.0")