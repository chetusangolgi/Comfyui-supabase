import numpy as np
from PIL import Image, ImageSequence, ImageOps
from io import BytesIO
from supabase import create_client
import requests
import torch

class SupabaseTableWatcherNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "supabase_url": ("STRING", {"default": "https://your-project.supabase.co"}),
                "supabase_key": ("STRING", {"default": "your-anon-key"}),
                "table_name": ("STRING", {"default": "inputimagetable"}),
                "image_column": ("STRING", {"default": "image_url"}),
                "id_column": ("STRING", {"default": "unique_id"}),
                "unique_id": ("STRING", {"default": ""})
            }
        }

    RETURN_TYPES = ("IMAGE", "MASK", "STRING")
    RETURN_NAMES = ("image", "mask", "unique_id")
    FUNCTION = "start_watcher"

    CATEGORY = "Custom/Supabase"

    def start_watcher(self, supabase_url, supabase_key, table_name, image_column, id_column, unique_id):
        print("[SupabaseNode] Starting fetch from Supabase...")

        # Stateless supabase client creation
        supabase = create_client(supabase_url, supabase_key)

        # Fetch the record matching the unique_id
        try:
            response = (
                supabase
                .table(table_name)
                .select(f"{image_column},{id_column}")
                .eq(id_column, unique_id)
                .limit(1)
                .execute()
            )
        except Exception as e:
            raise RuntimeError(f"[SupabaseNode] Error querying Supabase: {e}")

        if not response.data:
            raise ValueError(f"No image data found in Supabase for {id_column}={unique_id}.")

        image_url = response.data[0][image_column]
        fetched_unique_id = response.data[0][id_column]
        print(f"[SupabaseNode] Fetched image URL: {image_url} with {id_column}: {fetched_unique_id}")

        img = self.load_image(image_url)
        img_out, mask_out = self.pil2tensor(img)

        return (img_out, mask_out, fetched_unique_id)

    def load_image(self, image_source):
        if image_source.startswith('http'):
            response = requests.get(image_source)
            img = Image.open(BytesIO(response.content))
        else:
            img = Image.open(image_source)
        return img

    def pil2tensor(self, img):
        output_images = []
        output_masks = []

        for i in ImageSequence.Iterator(img):
            i = ImageOps.exif_transpose(i)
            if i.mode == 'I':
                i = i.point(lambda i: i * (1 / 255))
            image = i.convert("RGB")
            image = np.array(image).astype(np.float32) / 255.0
            image = torch.from_numpy(image)[None,]

            if 'A' in i.getbands():
                mask = np.array(i.getchannel('A')).astype(np.float32) / 255.0
                mask = 1. - torch.from_numpy(mask)
            else:
                mask = torch.zeros((i.size[1], i.size[0]), dtype=torch.float32)

            output_images.append(image)
            output_masks.append(mask.unsqueeze(0))

        if len(output_images) > 1:
            output_image = torch.cat(output_images, dim=0)
            output_mask = torch.cat(output_masks, dim=0)
        else:
            output_image = output_images[0]
            output_mask = output_masks[0]

        return (output_image, output_mask)
