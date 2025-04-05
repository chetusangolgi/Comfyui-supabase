from .load_image import SupabaseTableWatcherNode
from .upload_image import SupabaseImageUploader

NODE_CLASS_MAPPINGS = {
    "SupabaseTableWatcherNode": SupabaseTableWatcherNode,
    "SupabaseImageUploader": SupabaseImageUploader,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "SupabaseTableWatcherNode": "Supabase Table Watcher",
    "SupabaseImageUploader": "Upload Image to Supabase",
}
