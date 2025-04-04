import numpy as np
from PIL import Image
from io import BytesIO
import time
from supabase import create_client
import re

class SupabaseImageUploader:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "supabase_url": ("STRING", {"default": "https://your-project.supabase.co"}),
                "supabase_key": ("STRING", {"default": "your-service-role-key"}),
                "bucket": ("STRING", {"default": "images"}),
                "base_file_name": ("STRING", {"default": "image"})  # like 'image' → will become image_1.png
            }
        }

    RETURN_TYPES = ()
    FUNCTION = "upload"

    CATEGORY = "Custom/Supabase"

    def upload(self, image, supabase_url, supabase_key, bucket, base_file_name):
        try:
            # Convert tensor to PIL image
            img_np = (image[0] * 255).clip(0, 255).astype(np.uint8)
            img_pil = Image.fromarray(img_np)

            buffer = BytesIO()
            img_pil.save(buffer, format="PNG")
            buffer.seek(0)

            supabase = create_client(supabase_url, supabase_key)

            # List existing files to determine increment
            files = supabase.storage.from_(bucket).list()

            max_index = 0
            pattern = re.compile(f"^{re.escape(base_file_name)}_(\\d+)\\.png$")
            for file in files:
                match = pattern.match(file['name'])
                if match:
                    index = int(match.group(1))
                    max_index = max(max_index, index)

            next_index = max_index + 1
            filename = f"{base_file_name}_{next_index}.png"

            # Upload to Supabase
            result = supabase.storage.from_(bucket).upload(filename, buffer.read(), {"content-type": "image/png"})
            print(f"[Uploader] Uploaded as {filename}")
            return ()

        except Exception as e:
            print("[Uploader] Error uploading to Supabase:", e)
            return ()
