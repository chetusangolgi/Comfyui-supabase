import numpy as np
from PIL import Image
from io import BytesIO
from supabase import create_client
import datetime

class SupabaseImageUploader:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "id": ("ID",),
                "supabase_url": ("STRING", {"default": "https://your-project.supabase.co"}),
                "supabase_key": ("STRING", {"default": "your-service-role-key"}),
                "bucket": ("STRING", {"default": "images"}),
                "base_file_name": ("STRING", {"default": "image"})
            }
        }

    RETURN_TYPES = ()
    RETURN_NAMES = ()
    FUNCTION = "upload"
    CATEGORY = "Custom/Supabase"
    OUTPUT_NODE = True  # This tells ComfyUI this is an output node

    def upload(self, image,id, supabase_url, supabase_key, bucket, base_file_name):
        result = {"success": False, "message": "", "filename": ""}
        
        try:
            # Handle batched input (take first image if batch)
            if len(image.shape) == 4:
                image = image[0]
                
            # Convert tensor to numpy array
            img_np = image.cpu().numpy() if hasattr(image, 'cpu') else np.array(image)
            img_np = (img_np * 255).clip(0, 255).astype(np.uint8)
            
            # Convert to RGB if needed
            if img_np.shape[-1] == 1:
                img_np = np.repeat(img_np, 3, axis=-1)
            elif img_np.shape[-1] > 3:
                img_np = img_np[:, :, :3]
                
            # Create PIL Image
            img_pil = Image.fromarray(img_np)
            
            # Save to buffer
            buffer = BytesIO()
            img_pil.save(buffer, format="PNG")
            buffer.seek(0)
            
            # Create Supabase client
            supabase = create_client(supabase_url, supabase_key)
            
            # Generate filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{base_file_name}_{timestamp}.png"
            
            # Upload to Supabase
            res = supabase.storage.from_(bucket).upload(
                file=buffer.read(),
                path=filename,
                file_options={"content-type": "image/png"}
            )
            

            # Save image URL to database (optional)
            image_url = f"{supabase_url}/storage/v1/object/public/{bucket}/{filename}"
            #save image URL to database for the given ID
            response = supabase.table("userInfo").update({"output-image": image_url}).eq("id", id).execute()
            
            result["success"] = True
            result["message"] = "Upload successful"
            result["filename"] = filename
            print(f"[SupabaseUploader] Uploaded {filename} to bucket {bucket}")
            
        except Exception as e:
            result["message"] = str(e)
            print(f"[SupabaseUploader] Error: {e}")
            
        return result
