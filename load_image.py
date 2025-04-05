import threading
import time
import numpy as np
from PIL import Image
from io import BytesIO
from supabase import create_client, Client
import requests


class SupabaseWatcherNode:
    def __init__(self):
        self.running = False
        self.latest_file = None
        # Initialize with a default black image
        self.output = np.zeros((1, 64, 64, 3), dtype=np.float32)
        self.thread = None
        self.last_poll_time = 0
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

    RETURN_TYPES = ("IMAGE",)
    RETURN_NAMES = ("image",)
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

        # Always return a valid image array
        return (self.output,)

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
                        url = self.supabase.storage.from_(self.bucket_name).get_public_url(latest)
                        print(f"[SupabaseWatcherNode] Downloading from URL: {url}")
                        new_image = self.download_and_prepare_image(url)
                        if new_image is not None:
                            self.output = new_image
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
            img = Image.open(BytesIO(response.content)).convert("RGB")
            img = np.array(img).astype(np.float32) / 255.0
            
            # Ensure correct shape (1, H, W, 3)
            if len(img.shape) == 3:
                img = img[None,]
            return img
        except Exception as e:
            print(f"[SupabaseWatcherNode] Error loading image: {e}")
            # Return None to keep the current image instead of updating with an error
            return None

    @classmethod
    def IS_CHANGED(cls, **kwargs):
        return True  # always run when called

    def IS_DIRTY(self, *args, **kwargs):
        if self._should_rerun:
            self._should_rerun = False  # reset the flag
            return True
        return False

    def __del__(self):
        self.running = False
        if self.thread is not None:
            self.thread.join(timeout=1.0)
