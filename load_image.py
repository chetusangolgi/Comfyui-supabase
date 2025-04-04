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
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self.poll_loop, args=(supabase_url, supabase_key, bucket_name, poll_interval), daemon=True)
            self.thread.start()
        return (self.output,) if self.output is not None else (None,)

    def poll_loop(self, url, key, bucket, interval):
        supabase: Client = create_client(url, key)
        while self.running:
            try:
                files = supabase.storage.from_(bucket).list()
                if files:
                    sorted_files = sorted(files, key=lambda x: x['created_at'], reverse=True)
                    latest = sorted_files[0]['name']

                    if latest != self.latest_file:
                        self.latest_file = latest
                        print(f"New image detected: {latest}")
                        self.output = self.download_and_prepare_image(supabase.storage.from_(bucket).get_public_url(latest))
                time.sleep(interval)
            except Exception as e:
                print("Supabase polling error:", e)
                time.sleep(interval)

    def download_and_prepare_image(self, image_url):
        try:
            response = requests.get(image_url)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert("RGB")
            img = np.array(img).astype(np.float32) / 255.0
            return img[None,]
        except Exception as e:
            print("Error loading image:", e)
            return None

    @classmethod
    def IS_CHANGED(cls, **kwargs):
        return True
