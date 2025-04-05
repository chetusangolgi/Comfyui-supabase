import threading
import time
import numpy as np
from PIL import Image
from io import BytesIO
from supabase import create_client, Client
import requests
import torch


class SupabaseWatcherNode:
    def __init__(self):
        self.running = False
        self.latest_file = None
        self.output_image = torch.zeros((1, 64, 64, 3), dtype=torch.float32)
        self.output_mask = torch.zeros((1, 64, 64), dtype=torch.float32)
        self.thread = None
        self._should_rerun = False
        self.supabase = None

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "supabase_url": ("STRING", {"default": "https://your-project.supabase.co"}),
                "supabase_key": ("STRING", {"default": "your-service-role-key"}),
                "bucket_name": ("STRING", {"default": "images"}),
                "poll_interval": ("INT", {"default": 10, "min": 2, "max": 60}),
            }
        }

    RETURN_TYPES = ("IMAGE", "MASK")
    RETURN_NAMES = ("image", "mask")
    FUNCTION = "start_watcher"

    CATEGORY = "Custom/Supabase"

    def start_watcher(self, supabase_url, supabase_key, bucket_name, poll_interval):
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.bucket_name = bucket_name
        self.poll_interval = poll_interval

        if not self.running:
            self.running = True
            try:
                self.supabase = create_client(supabase_url, supabase_key)
                self.thread = threading.Thread(target=self.poll_loop, daemon=True)
                self.thread.start()
            except Exception as e:
                print(f"[SupabaseWatcherNode] Error initializing: {e}")

        return (self.output_image, self.output_mask)

    def poll_loop(self):
        while self.running:
            try:
                print("[SupabaseWatcherNode] Polling for new images...")
                files = self.supabase.storage.from_(self.bucket_name).list()
                if files:
                    sorted_files = sorted(files, key=lambda x: x['created_at'], reverse=True)
                    latest = sorted_files[0]['name']
                    print(f"[SupabaseWatcherNode] Found {len(files)} files, latest: {latest}")

                    if latest != self.latest_file:
                        self.latest_file = latest
                        print(f"[SupabaseWatcherNode] New image detected: {latest}")

                        # Public bucket URL
                        url = f"{self.supabase_url}/storage/v1/object/public/{self.bucket_name}/{latest}"
                        print(f"[SupabaseWatcherNode] Downloading from: {url}")

                        image_tensor, mask_tensor = self.download_and_prepare_image(url)
                        if image_tensor is not None:
                            self.output_image = image_tensor
                            self.output_mask = mask_tensor
                            self._should_rerun = True
                else:
                    print("[SupabaseWatcherNode] No files found in bucket")
                time.sleep(self.poll_interval)
            except Exception as e:
                print(f"[SupabaseWatcherNode] Error in poll loop: {str(e)}")
                time.sleep(self.poll_interval)

    def download_and_prepare_image(self, image_url):
        try:
            response = requests.get(image_url)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert("RGBA")  # force RGBA

            img_np = np.array(img).astype(np.float32) / 255.0
            rgb = img_np[..., :3]
            alpha = img_np[..., 3]

            image_tensor = torch.from_numpy(rgb)[None, ...]  # shape: (1, H, W, 3)
            mask_tensor = 1.0 - torch.from_numpy(alpha)[None, ...]  # shape: (1, H, W)

            return image_tensor, mask_tensor
        except Exception as e:
            print(f"[SupabaseWatcherNode] Error loading image: {e}")
            return None, None

    @classmethod
    def IS_CHANGED(cls, **kwargs):
        return True

    def IS_DIRTY(self, *args, **kwargs):
        if self._should_rerun:
            self._should_rerun = False
            return True
        return False

    def __del__(self):
        self.running = False
        if self.thread is not None:
            self.thread.join(timeout=1.0)
