import numpy as np
from PIL import Image
from io import BytesIO
import time
from supabase import create_client

class SupabaseImageUploader:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "file_name": ("STRING", {"default": "generated_image.png"}),
                "supabase_url": ("STRING", {"default": "https://your-project.supabase.co"}),
                "supabase_key": ("STRING", {"default": "your-service-role-key"}),
                "bucket": ("STRING", {"default": "images"})
            }
        }

    RETURN_TYPES = ()
    FUNCTION = "upload"

    CATEGORY = "Custom/Supabase"

    def upload(self, image, file_name, supabase_url, supabase_key, bucket):
        # Convert image from tensor to PIL
        try:
            img_np = (image[0] * 255).clip(0, 255).astype(np.uint8)
            img_pil = Image.fromarray(img_np)

            buffer = BytesIO()
            img_pil.save(buffer, format="PNG")
            buffer.seek(0)

            # Connect to Supabase
            supabase = create_client(supabase_url, supabase_key)

            # Create unique file name with timestamp
            timestamp = int(time.time())
            path_on_supabase = f"{timestamp}_{file_name}"

            # Upload to Supabase bucket
            res = supabase.storage().from_(bucket).upload(path_on_supabase, buffer.read(), {"content-type": "image/png"})

            print(f"Uploaded to Supabase at path: {path_on_supabase}")
            return ()
        except Exception as e:
            print("Error uploading to Supabase:", e)
            return ()

