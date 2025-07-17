from .load_image import SupabaseTableWatcherNode
from .upload_image import SupabaseImageUploader
from .upload_audio import SupabaseAudioUploader  # ✅ Add this line

NODE_CLASS_MAPPINGS = {
    "SupabaseTableWatcherNode": SupabaseTableWatcherNode,
    "SupabaseImageUploader": SupabaseImageUploader,
    "SupabaseAudioUploader": SupabaseAudioUploader,  # ✅ Add this line
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "SupabaseTableWatcherNode": "Supabase Table Watcher",
    "SupabaseImageUploader": "Upload Image to Supabase",
    "SupabaseAudioUploader": "Upload Audio to Supabase",  # ✅ Add this line
}
