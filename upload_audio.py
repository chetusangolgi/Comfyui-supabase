import datetime
from io import BytesIO
import numpy as np
from pydub import AudioSegment
from supabase import create_client

class SupabaseAudioUploader:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "audio": ("AUDIO",),  # ComfyUI passes a dict with 'waveform' and 'sample_rate'
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
            print(f"[SupabaseAudioUploader] Received audio type: {type(audio)}")
            print(f"[SupabaseAudioUploader] Audio dict keys: {list(audio.keys())}")

            if not isinstance(audio, dict) or "waveform" not in audio or "sample_rate" not in audio:
                raise ValueError("Expected audio dict with 'waveform' and 'sample_rate' keys")

            waveform = audio["waveform"]
            sample_rate = audio["sample_rate"]

            # Convert to NumPy
            if hasattr(waveform, "cpu"):
                waveform = waveform.cpu().numpy()

            # waveform shape: (1, channels, samples)
            if len(waveform.shape) == 3:
                waveform = waveform[0]  # remove batch dim

            channels = waveform.shape[0]
            samples = waveform.shape[1]
            print(f"[SupabaseAudioUploader] Audio shape: {waveform.shape}, channels: {channels}, samples: {samples}")

            # Reshape to (samples, channels) for pydub
            audio_np = waveform.T.astype(np.float32)
            audio_int16 = (audio_np * 32767.0).clip(-32768, 32767).astype(np.int16)

            # Flatten to mono if channels == 1
            if channels == 1:
                audio_bytes = audio_int16.tobytes()
            else:
                audio_bytes = audio_int16.reshape(-1, channels).tobytes()

            audio_segment = AudioSegment(
                audio_bytes,
                frame_rate=sample_rate,
                sample_width=2,  # int16 = 2 bytes
                channels=channels
            )

            buffer = BytesIO()
            audio_segment.export(buffer, format="mp3")
            buffer.seek(0)

            # Supabase upload
            supabase = create_client(supabase_url, supabase_key)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{base_file_name}_{timestamp}.mp3"

            upload_response = supabase.storage.from_(bucket).upload(
                file=buffer.read(),
                path=filename,
                file_options={"content-type": "audio/mpeg"}
            )

            if hasattr(upload_response, "status_code") and upload_response.status_code not in [200, 201]:
                result["message"] = f"Upload failed: {getattr(upload_response, 'data', upload_response)}"
                print(f"[SupabaseAudioUploader] Error: {result['message']}")
                return result

            public_url = supabase.storage.from_(bucket).get_public_url(filename)

            if unique_id and public_url:
                try:
                    update_data = {table_column: public_url}
                    supabase.table(table_name).update(update_data).eq(unique_id_column, unique_id).execute()
                    print(f"[SupabaseAudioUploader] Updated {table_name} for {unique_id_column}={unique_id}")
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
