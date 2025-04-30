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
                "unique_id": ("STRING",),
                "supabase_url": ("STRING", {"default": "https://your-project.supabase.co"}),
                "supabase_key": ("STRING", {"default": "your-service-role-key"}),
                "bucket": ("STRING", {"default": "outputimages"}),
                "base_file_name": ("STRING", {"default": "image"}),
                "table_name": ("STRING", {"default": "inputimagetable"}),
                "unique_id_column": ("STRING", {"default": "unique_id"}),
                "table_column": ("STRING", {"default": "output"})
            }
        }

    RETURN_TYPES = ()
    RETURN_NAMES = ()
    FUNCTION = "upload"
    CATEGORY = "Custom/Supabase"
    OUTPUT_NODE = True  # This tells ComfyUI this is an output node

    def upload(self, image, supabase_url, supabase_key, bucket, base_file_name, table_name, unique_id, unique_id_column, table_column):
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
            upload_response = supabase.storage.from_(bucket).upload(
                file=buffer.read(),
                path=filename,
                file_options={"content-type": "image/png"}
            )

            # Check if upload was successful before proceeding
            if hasattr(upload_response, "status_code") and upload_response.status_code not in [200, 201]:
                result["message"] = f"Upload failed: {getattr(upload_response, 'data', upload_response)}"
                print(f"[SupabaseUploader] Error: {result['message']}")
                return result

            # Get the public URL for the uploaded image
            public_url = supabase.storage.from_(bucket).get_public_url(filename)

            # Only update the table if the public_url is not None or empty
            if unique_id and public_url:
                try:
                    update_data = {table_column: public_url}
                    update_response = (
                        supabase.table(table_name)
                        .update(update_data)
                        .eq(unique_id_column, unique_id)
                        .execute()
                    )
                    print(f"[SupabaseUploader] Updated table {table_name} for {unique_id_column}={unique_id} with {table_column} URL")
                except Exception as e:
                    print(f"[SupabaseUploader] Error updating table: {e}")
            
            result["success"] = True
            result["message"] = "Upload successful"
            result["filename"] = filename
            result["public_url"] = public_url
            print(f"[SupabaseUploader] Uploaded {filename} to bucket {bucket}")
            
        except Exception as e:
            result["message"] = str(e)
            print(f"[SupabaseUploader] Error: {e}")
            
        return result
