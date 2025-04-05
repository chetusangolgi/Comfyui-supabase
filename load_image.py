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
                "supabase_url": ("STRING", {"default": "https://your-project.supabase.co"}),
                "supabase_key": ("STRING", {"default": "your-service-role-key"}),
                "table_name": ("STRING", {"default": "image_table"}),
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

        if not self.running:
            self.running = True
            try:
                self.supabase = create_client(supabase_url, supabase_key)
                self.thread = threading.Thread(target=self.poll_loop, daemon=True)
                self.thread.start()
            except Exception as e:
                print(f"[SupabaseTableWatcherNode] Error initializing: {e}")

        return (self.output_image, self.output_mask)

    def poll_loop(self):
        while self.running:
            try:
                print("[SupabaseTableWatcherNode] Polling table for new entries...")
                response = self.supabase.table(self.table_name).select("*").order("id", desc=True).limit(1).execute()

                if response.data:
                    row = response.data[0]
                    row_id = row.get("id")
                    image_url = row.get(self.image_column)

                    if row_id != self.latest_row_id and image_url:
                        self.latest_row_id = row_id
                        print(f"[SupabaseTableWatcherNode] New row detected with ID: {row_id}")
                        print(f"[SupabaseTableWatcherNode] Image URL: {image_url}")

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
