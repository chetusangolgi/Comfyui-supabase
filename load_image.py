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
        self.output = None
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
            self.supabase = create_client(supabase_url, supabase_key)
            self.thread = threading.Thread(target=self.poll_loop, daemon=True)
            self.thread.start()

        # Return the latest image (if available)
        return (self.output,) if self.output is not None else (None,)

    def poll_loop(self):
        while self.running:
            try:
                files = self.supabase.storage.from_(self.bucket_name).list()
                if files:
                    sorted_files = sorted(files, key=lambda x: x['created_at'], reverse=True)
                    latest = sorted_files[0]['name']

                    if latest != self.latest_file:
                        self.latest_file = latest
                        print(f"[SupabaseWatcherNode] New image detected: {latest}")
                        self.output = self.download_and_prepare_image(
                            self.supabase.storage.from_(self.bucket_name).get_public_url(latest)
                        )
                        self._should_rerun = True  # Flag to re-execute the graph

                time.sleep(self.poll_interval)
            except Exception as e:
                print("[SupabaseWatcherNode] Error in poll loop:", e)
                time.sleep(self.poll_interval)

    def download_and_prepare_image(self, image_url):
        try:
            response = requests.get(image_url)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert("RGB")
            img = np.array(img).astype(np.float32) / 255.0
            return img[None,]
        except Exception as e:
            print("[SupabaseWatcherNode] Error loading image:", e)
            return None

    @classmethod
    def IS_CHANGED(cls, **kwargs):
        return True  # always run when called

    def IS_DIRTY(self, *args, **kwargs):
        if self._should_rerun:
            self._should_rerun = False  # reset the flag
            return True
        return False
