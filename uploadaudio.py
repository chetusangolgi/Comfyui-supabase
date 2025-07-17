import numpy as np
from io import BytesIO
import datetime
import soundfile as sf  # pip install soundfile
from supabase import create_client

class SupabaseAudioUploader:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "audio": ("AUDIO",),  # This must be supported by your ComfyUI extension
                "unique_id": ("STRING",),
                "supabase_url": ("STRING", {"default": "https://your-project.supabase.co"}),
                "supabase_key": ("STRING", {"default": "your-service-role-key"}),
                "bucket": ("STRING", {"default": "outputaudio"}),
                "base_file_name": ("STRING", {"default": "audio"}),
                "table_name": ("STRING", {"default": "inputaudiotable"}),
                "unique_id_column": ("STRING", {"default": "unique_id"}),
                "table_column": ("STRING", {"default": "output"})
            }
        }

    RETURN_TYPES = ()
    RETURN_NAMES = ()
    FUNCTION = "upload"
    CATEGORY = "Custom/Supabase"
    OUTPUT_NODE = True

    def upload(self, audio, supabase_url, supabase_key, bucket, base_file_name, table_name, unique_id, unique_id_column, table_column):
        result = {"success": False, "message": "", "filename": ""}

        try:
            # Unpack audio object (assuming a tuple of (data, sample_rate))
            audio_data, sample_rate = audio  # Ensure input node provides it this way
            
            if isinstance(audio_data, np.ndarray):
                buffer = BytesIO()
                sf.write(buffer, audio_data, sample_rate, format='WAV')
                buffer.seek(0)
            else:
                raise ValueError("Audio input is not a valid NumPy array with sample rate")

            # Create Supabase client
            supabase = create_client(supabase_url, supabase_key)

            # Generate filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{base_file_name}_{timestamp}.wav"

            # Upload to Supabase
            upload_response = supabase.storage.from_(bucket).upload(
                file=buffer.read(),
                path=filename,
                file_options={"content-type": "audio/wav"}
            )

            if hasattr(upload_response, "status_code") and upload_response.status_code not in [200, 201]:
                result["message"] = f"Upload failed: {getattr(upload_response, 'data', upload_response)}"
                print(f"[SupabaseAudioUploader] Error: {result['message']}")
                return result

            public_url = supabase.storage.from_(bucket).get_public_url(filename)

            if unique_id and public_url:
                try:
                    update_data = {table_column: public_url}
                    update_response = (
                        supabase.table(table_name)
                        .update(update_data)
                        .eq(unique_id_column, unique_id)
                        .execute()
                    )
                    print(f"[SupabaseAudioUploader] Updated table {table_name} for {unique_id_column}={unique_id} with {table_column} URL")
                except Exception as e:
                    print(f"[SupabaseAudioUploader] Error updating table: {e}")

            result["success"] = True
            result["message"] = "Upload successful"
            result["filename"] = filename
            result["public_url"] = public_url
            print(f"[SupabaseAudioUploader] Uploaded {filename} to bucket {bucket}")

        except Exception as e:
            result["message"] = str(e)
            print(f"[SupabaseAudioUploader] Error: {e}")

        return result
