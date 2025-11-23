import torch
from torchvision.models import vit_b_16, ViT_B_16_Weights
from torchvision import transforms
from PIL import Image
import numpy as np
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ImageFeatureExtractor:
    def __init__(self, device: Optional[str] = None):
        if device is None:
            self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        else:
            self.device = device
            
        logger.info(f"Using device: {self.device}")
        
        # Initialize ViT model
        self.model = vit_b_16(weights=ViT_B_16_Weights.IMAGENET1K_V1)
        
        # Modify forward pass to get embeddings
        self.original_forward = self.model.forward
        self.model.forward = self._forward_features
        
        self.model.eval()
        self.model.to(self.device)
        
        self.feature_dim = 768  # Fixed for ViT-B/16
        
        self.transform = transforms.Compose([
            transforms.Resize(224),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406],
                               std=[0.229, 0.224, 0.225])
        ])

    def _forward_features(self, x):
        x = self.model._process_input(x)
        n = x.shape[0]
        cls_token = self.model.class_token.expand(n, -1, -1)
        x = torch.cat([cls_token, x], dim=1)
        x = self.model.encoder(x)
        return x[:, 0]

    @torch.no_grad()
    def extract_features(self, image_path: str) -> np.ndarray:
        try:
            image = Image.open(image_path).convert('RGB')
            image = self.transform(image).unsqueeze(0).to(self.device)
            features = self.model(image)
            features = features.cpu().numpy().squeeze()
            
            # L2 Normalize
            norm = np.linalg.norm(features)
            if norm > 0:
                features = features / norm
            
            # CRITICAL FIX: Cast to float32 to prevent FAISS crash
            return features.astype(np.float32)
        except Exception as e:
            logger.error(f"Error extracting features from {image_path}: {str(e)}")
            raise