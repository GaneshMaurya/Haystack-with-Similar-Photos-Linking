import os
import json
import faiss
import numpy as np
from datetime import datetime
import logging
from typing import List, Tuple, Optional
# FIX: Correct import for Docker environment
from feature_extractor import ImageFeatureExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ImageRetrievalSystem:
    def __init__(self, 
                 index_path: Optional[str] = None,
                 metadata_path: Optional[str] = None,
                 use_gpu: bool = False,
                 n_regions: int = 100,
                 nprobe: int = 10):
        
        self.feature_extractor = ImageFeatureExtractor()
        self.feature_dim = self.feature_extractor.feature_dim
        self.index_path = index_path
        self.metadata_path = metadata_path
        self.metadata = {}
        self.is_trained = True # Flat index is always trained
        self.use_gpu = use_gpu

        # Load existing or create new
        if index_path and metadata_path and os.path.exists(index_path) and os.path.exists(metadata_path):
            self.load(index_path, metadata_path)
        else:
            logger.info("Initializing Simple Flat L2 Index (No training required)")
            self.index = faiss.IndexFlatL2(self.feature_dim)
            if self.use_gpu:
                self._to_gpu()

    def _to_gpu(self):
        if self.use_gpu:
            try:
                res = faiss.StandardGpuResources()
                self.index = faiss.index_cpu_to_gpu(res, 0, self.index)
            except Exception as e:
                logger.warning(f"GPU failed, using CPU: {e}")

    def index_images(self, image_dir: str):
        """Batch index a directory."""
        image_paths = [os.path.join(image_dir, f) for f in os.listdir(image_dir) 
                       if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        
        if not image_paths:
            return

        for path in image_paths:
            try:
                feat = self.feature_extractor.extract_features(path)
                self.index.add(np.array([feat]))
                
                new_id = self.index.ntotal - 1
                self.metadata[str(new_id)] = {
                    'photo_id': os.path.basename(path),
                    'path': path,
                    'timestamp': datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Skipping {path}: {e}")
        
        logger.info(f"Indexed {len(image_paths)} images.")

    def add_single_image(self, image_path: str, photo_id: str):
        """Add one image to the index."""
        features = self.feature_extractor.extract_features(image_path)
        self.index.add(np.array([features]))
        
        new_id = self.index.ntotal - 1
        self.metadata[str(new_id)] = {
            'photo_id': photo_id,
            'path': image_path,
            'timestamp': datetime.now().isoformat()
        }
        
        if self.index_path and self.metadata_path:
            self.save(self.index_path, self.metadata_path)
            
        return new_id

    def search(self, query_path: str, k: int = 5):
        query_feat = self.feature_extractor.extract_features(query_path)
        k = min(k, self.index.ntotal)
        
        distances, indices = self.index.search(query_feat.reshape(1, -1), k)
        
        results = []
        for dist, idx in zip(distances[0], indices[0]):
            s_idx = str(int(idx))
            if s_idx in self.metadata:
                item = self.metadata[s_idx]
                results.append({
                    "photo_id": item['photo_id'],
                    "distance": float(dist),
                    "path": item['path']
                })
        
        return sorted(results, key=lambda x: x['distance'])

    def find_path_by_id(self, photo_id: str):
        # Allow string or int comparison for robustness
        for item in self.metadata.values():
            if str(item['photo_id']) == str(photo_id):
                return item['path']
        return None

    def save(self, index_path, meta_path):
        idx_to_save = faiss.index_gpu_to_cpu(self.index) if self.use_gpu else self.index
        faiss.write_index(idx_to_save, index_path)
        with open(meta_path, 'w') as f:
            json.dump(self.metadata, f)

    def load(self, index_path, meta_path):
        self.index = faiss.read_index(index_path)
        with open(meta_path, 'r') as f:
            self.metadata = json.load(f)
        if self.use_gpu:
            self._to_gpu()