import threading
import time
import numpy as np
from PIL import Image
from io import BytesIO
from supabase import create_client, Client
import requests
import torch


class SupabaseTableWatcherNode:
    def __init__(self):
        self.running = False
        self.latest_row_id = None
        self.output_image = torch.zeros((1, 64, 64, 3), dtype=torch.float32)
        self.output_mask = torch.zeros((1, 64, 64), dtype=torch.float32)
        self.thread = None
        self._should_rerun = False
        self.supabase = None

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "supabase_url": ("STRING", {"default": "https://popppjirsdedxhetcphs.supabase.co"}),
                "supabase_key": ("STRING", {"default": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBvcHBwamlyc2RlZHhoZXRjcGhzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDM3NjMxMDAsImV4cCI6MjA1OTMzOTEwMH0.Ihv60cbfUSeDN5dPDsOZRz4y79ek3D5YZZgKwBsMkSc"}),
                "table_name": ("STRING", {"default": "inputimagetable"}),
                "image_column": ("STRING", {"default": "image_url"}),
                "poll_interval": ("INT", {"default": 10, "min": 2, "max": 60}),
            }
        }

    RETURN_TYPES = ("IMAGE", "MASK")
    RETURN_NAMES = ("image", "mask")
    FUNCTION = "start_watcher"

    CATEGORY = "Custom/Supabase"

    def start_watcher(self, supabase_url, supabase_key, table_name, image_column, poll_interval):
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.table_name = table_name
        self.image_column = image_column
        self.poll_interval = poll_interval
        self.order_by_column = "created_at"  

        if not self.running:
            self.running = True
            try:
                self.supabase = create_client(self.supabase_url, self.supabase_key)
                print("client created")
                self.thread = threading.Thread(target=self.poll_loop, daemon=True)
                self.thread.start()
            except Exception as e:
                print(f"[SupabaseTableWatcherNode] Error initializing: {e}")
                return (self.output_image, self.output_mask)

    def poll_loop(self):
        while self.running:
            try:
                print("[SupabaseTableWatcherNode] Polling table for new entries...")
                response =(
                    self.supabase.table(self.table_name)
                    .select(self.image_column)
                    .order(self.order_by_column, desc=True)
                    .limit(1)
                    .execute()
                    )

                if response.data:
                    image_url = response.data[0][self.image_column]
            
                    print(f"[SupabaseTableWatcherNode] Latest image URL: {image_url}")
                    
                    image_tensor, mask_tensor = self.download_and_prepare_image(image_url)
                    if image_tensor is not None:
                        self.output_image = image_tensor
                        self.output_mask = mask_tensor
                        self._should_rerun = True
                else:
                    print("[SupabaseTableWatcherNode] No rows found.")

                time.sleep(self.poll_interval)
            except Exception as e:
                print(f"[SupabaseTableWatcherNode] Error in poll loop: {e}")
                time.sleep(self.poll_interval)

    def download_and_prepare_image(self, image_url):
        try:
            response = requests.get(image_url)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert("RGBA")

            img_np = np.array(img).astype(np.float32) / 255.0
            rgb = img_np[..., :3]
            alpha = img_np[..., 3]

            image_tensor = torch.from_numpy(rgb)[None, ...]
            mask_tensor = 1.0 - torch.from_numpy(alpha)[None, ...]

            return image_tensor, mask_tensor
        except Exception as e:
            print(f"[SupabaseTableWatcherNode] Error loading image: {e}")
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
