from .load_image import SupabaseWatcherNode
from .upload_image import SupabaseImageUploader

NODE_CLASS_MAPPINGS = {
    "SupabaseWatcherNode": SupabaseWatcherNode,
    "SupabaseImageUploader": SupabaseImageUploader,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "SupabaseWatcherNode": "Watch Supabase Bucket",
    "SupabaseImageUploader": "Upload Image to Supabase",
}
