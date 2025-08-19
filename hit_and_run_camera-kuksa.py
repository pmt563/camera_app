import os
import cv2
import yaml
import json
import time
import queue
import shutil
import threading
import requests
import urllib3
import subprocess
import concurrent.futures
from datetime import datetime
from kuksa_client.grpc import VSSClient, Datapoint
import paho.mqtt.client as mqtt
 
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
 
# === Load config ===
with open('config/record.yaml', 'r') as f:
    record_config = yaml.safe_load(f)
 
WATCH_FOLDER = record_config['base_directory']
VIDEO_EXTENSIONS = ('.mp4', '.webm')
UPLOAD_STAGE_PATH = "Vehicle.Connectivity.Emergency.Upload.Stage"
UPLOAD_STATUS_PATH = "Vehicle.Connectivity.Emergency.Upload.Status"
MAX_RETRIES = 3
SERVER_UPLOAD_URL = 'https://34.127.65.112:8000/upload'
UPLOADED_TRACK_FILE = 'uploaded_folders.json'
 
KUKSA_HOST = "localhost"
KUKSA_PORT = 55555
 
MQTT_BROKER = "34.127.65.112"
MQTT_PORT = 1883
MQTT_TOPIC = "time_crash"
 
camera_index = record_config['camera_index']
format_video_file = record_config['format_video']
height = record_config['resolution']['height']
width = record_config['resolution']['width']
resolution = (width, height)
 
# Record in MP4 (Windows-friendly)
fourcc = cv2.VideoWriter_fourcc(*'VP80')
 
CAMERA_PATHS = {
    'front': 'Vehicle.Exterior.Camera.Front.Activate',
    'rear': 'Vehicle.Exterior.Camera.Rear.Activate',
    'left': 'Vehicle.Exterior.Camera.LeftSide.Activate',
    'right': 'Vehicle.Exterior.Camera.RightSide.Activate'
}
 
# === MQTT Setup ===
mqtt_client = mqtt.Client()
try:
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()
    print("[MQTT] Connected to broker.")
except Exception as e:
    print(f"[ERROR] MQTT connection failed: {e}")
 
# === Shared Variables ===
stop_queue = queue.Queue(1)
active_cameras = {}
camera_status = {cam: 0 for cam in CAMERA_PATHS}
session_dir_var = {"path": None}
 
# Stage flag and lock
upload_stage_active = False
upload_stage_lock = threading.Lock()
 
# === Conversion Thread Pool & ffmpeg discovery ===
conversion_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
conversion_futures = {}
 
# Find ffmpeg in PATH (Linux apt install puts it here)
FFMPEG_BIN = shutil.which("ffmpeg") or "ffmpeg"  # fallback to name; will error if truly missing
 
 
def convert_to_webm(mp4_path):
    pass
#     """Convert MP4 to WebM using ffmpeg (keeps both) and wait until ready)."""
#     webm_path = os.path.splitext(mp4_path)[0] + ".webm"
 
#     if not shutil.which(FFMPEG_BIN):
#         print("[ERROR] ffmpeg not found in PATH. Install it with:")
#         print("       sudo apt update && sudo apt install ffmpeg")
#         return None
 
#     print(f"[CONVERT] Starting: {mp4_path} -> {webm_path}")
 
#     try:
#         subprocess.run(
#             [
#                 FFMPEG_BIN,
#                 "-y",
#                 "-i", mp4_path,
#                 "-c:v", "libvpx-vp9",
#                 "-b:v", "1M",
#                 "-pix_fmt", "yuv420p",
#                 "-c:a", "libopus",
#                 webm_path,
#             ],
#             check=True,
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE,
#         )
 
#         # Wait until file exists and has size > 0
#         start_time = time.time()
#         while (not os.path.exists(webm_path) or os.path.getsize(webm_path) == 0) and time.time() - start_time < 10:
#             print(f"[WAIT] Waiting for conversion of {os.path.basename(mp4_path)}...")
#             time.sleep(0.5)
 
#         if os.path.exists(webm_path) and os.path.getsize(webm_path) > 0:
#             print(f"[CONVERT] Successfully converted: {webm_path}")
#             return webm_path
#         else:
#             print(f"[ERROR] Conversion did not produce valid file: {webm_path}")
#     except FileNotFoundError:
#         print(f"[ERROR] ffmpeg not found (checked '{FFMPEG_BIN}').")
#     except subprocess.CalledProcessError as e:
#         err = e.stderr.decode(errors='ignore') if e.stderr else str(e)
#         print(f"[ERROR] ffmpeg conversion error: {e}\n--- stderr ---\n{err}")
#     return None
 
 
def start_conversion_async(mp4_path):
    pass
    # """Submit a background conversion task."""
    # future = conversion_executor.submit(convert_to_webm, mp4_path)
    # conversion_futures[mp4_path] = future
    # return future
 
 
def wait_for_conversion(mp4_path):
    # """Wait for a specific MP4 to finish converting before upload."""
    # future = conversion_futures.get(mp4_path)
    # if future:
    #     webm_path = future.result()  # Blocks until ffmpeg finishes
    #     retries = 0
    #     while retries < 5:
    #         if webm_path and os.path.exists(webm_path) and os.path.getsize(webm_path) > 0:
    #             return webm_path
    #         retries += 1
    #         time.sleep(1)
    # return None
    pass
 
 
# === Upload Helpers ===
def load_uploaded_folders():
    if os.path.exists(UPLOADED_TRACK_FILE):
        with open(UPLOADED_TRACK_FILE, 'r') as f:
            return set(json.load(f))
    return set()
 
 
def save_uploaded_folders(uploaded_set):
    with open(UPLOADED_TRACK_FILE, 'w') as f:
        json.dump(list(uploaded_set), f)
 
 
def publish_timestamp():
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        mqtt_client.publish(MQTT_TOPIC, f"Emergency video recorded at {timestamp}")
        print(f"[MQTT] Published timestamp: {timestamp}")
    except Exception as e:
        print(f"[ERROR] MQTT publish failed: {e}")
 
 
def get_videos_in_folder(folder_path):
    """
    Return ONE entry per video 'stem' (basename without extension).
    Prefer .webm if it exists; otherwise use .mp4. Sorted by mtime.
    """
    by_stem = {}  # stem -> (name, fullpath)
    for root, _, files in os.walk(folder_path):
        for file in files:
            low = file.lower()
            if low.endswith(".mp4") or low.endswith(".webm"):
                stem, _ext = os.path.splitext(file)
                full = os.path.join(root, file)
                chosen = by_stem.get(stem)
 
                if chosen is None:
                    by_stem[stem] = (file, full)
                else:
                    _, existing_full = chosen
                    # Prefer .webm when both exist
                    if low.endswith(".webm"):
                        by_stem[stem] = (file, full)
                    elif existing_full.lower().endswith(".webm"):
                        pass  # keep existing .webm
                    else:
                        # both .mp4 — keep newer one
                        if os.path.getmtime(full) > os.path.getmtime(existing_full):
                            by_stem[stem] = (file, full)
 
    items = list(by_stem.values())
    items.sort(key=lambda x: os.path.getmtime(x[1]))
    return items  # [(name, fullpath), ...]
 
 
def upload_video(full_path, video_name, client):
    retries = 0
    upload_success = False
 
    while retries < MAX_RETRIES and not upload_success:
        try:
            client.set_current_values({UPLOAD_STATUS_PATH: Datapoint(2)})
            print(f"[UPLOAD] Attempt {retries + 1}/{MAX_RETRIES} for {video_name}")
 
            if not os.path.exists(full_path) or os.path.getsize(full_path) == 0:
                print(f"[ERROR] File missing or empty: {full_path}")
                break
 
            with open(full_path, 'rb') as f:
                files = {'video': (video_name, f, 'video/webm')}
                response = requests.post(SERVER_UPLOAD_URL, files=files, verify=False, timeout=30)
 
                if response.status_code == 200:
                    print(f"[UPLOAD SUCCESS] {video_name}")
                    upload_success = True
                    client.set_current_values({UPLOAD_STATUS_PATH: Datapoint(0)})
                    publish_timestamp()
                    return True
                else:
                    print(f"[UPLOAD FAILED] Status: {response.status_code}")
                    print(f"Response: {response.text}")
                    raise Exception(f"Server returned status {response.status_code}")
 
        except Exception as e:
            print(f"[ERROR] Upload error: {str(e)}")
            retries += 1
            if retries < MAX_RETRIES:
                print("[RETRY] Waiting 1 second...")
                time.sleep(1)
 
    client.set_current_values({UPLOAD_STATUS_PATH: Datapoint(0)})
    print(f"[ERROR] Failed to upload {video_name} after {MAX_RETRIES} attempts")
    return False
 
 
def upload_folder(folder_path, folder_name, uploaded_folders, client):
    if folder_name in uploaded_folders:
        print(f"[SKIP] Folder {folder_name} already uploaded.")
        return
 
    print(f"[UPLOAD] Preparing to upload: {folder_name}")
    publish_timestamp()
 
    time.sleep(1)  # Ensure file handles are closed
 
    if not os.path.exists(folder_path):
        print(f"[ERROR] Folder not found: {folder_path}")
        return
 
    videos = get_videos_in_folder(folder_path)
    if not videos:
        print(f"[WARN] No videos found in folder {folder_name}")
        return
 
    upload_count = 0
    for video_name, full_path in videos:
        # If only MP4 exists, convert once; if WEBM exists, just upload it.
        if full_path.lower().endswith(".mp4"):
            webm_path = convert_to_webm(full_path)
            if not webm_path:
                print(f"[ERROR] Skipping upload for {video_name} (conversion failed)")
                continue
            full_path = webm_path
            video_name = os.path.basename(webm_path)
 
        if full_path.lower().endswith(".webm"):
            if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                if upload_video(full_path, video_name, client):
                    upload_count += 1
            else:
                print(f"[ERROR] File missing or empty: {full_path}")
 
    if upload_count > 0:
        uploaded_folders.add(folder_name)
        save_uploaded_folders(uploaded_folders)
        print(f"[DONE] Uploaded {upload_count} WebM videos from {folder_name}")
    else:
        print(f"[ERROR] No WebM videos uploaded from {folder_name}")
 
 
# === Camera Threads ===
def monitor_camera_status_blocking():
    """Only handle camera status here. Do NOT touch Upload.Stage here."""
    with VSSClient(KUKSA_HOST, KUKSA_PORT) as client:
        for updates in client.subscribe_current_values(CAMERA_PATHS.values()):
            for path, datapoint in updates.items():
                if datapoint is None:
                    continue
                cam = [k for k, v in CAMERA_PATHS.items() if v == path][0]
                new_status = int(datapoint.value)
                if camera_index[cam] == 10:
                    continue
                if new_status == 0 and camera_status[cam] in (1, 2):
                    if cam in active_cameras:
                        active_cameras[cam].release()
                        del active_cameras[cam]
                    camera_status[cam] = 0
                elif new_status in (1, 2):
                    if cam not in active_cameras:
                        cap = cv2.VideoCapture(camera_index[cam], cv2.CAP_V4L2)
                        time.sleep(2)  # allow device settle
                        if cap.isOpened():
                            active_cameras[cam] = cap
                            camera_status[cam] = new_status
                            print(f"[CAMERA] Opened {cam}")
                        else:
                            print(f"[ERROR] Cannot open camera {cam}")
                else:
                    camera_status[cam] = new_status
 
            # Stop check
            if not stop_queue.empty() and stop_queue.get():
                print("[THREAD] Monitor camera exiting")
                break
 
 
def uploader_thread():
    """Edge-triggered watcher for Upload.Stage (0->1 only)."""
    global upload_stage_active
    last_stage_val = 0
    with VSSClient(KUKSA_HOST, KUKSA_PORT) as client:
        for updates in client.subscribe_current_values([UPLOAD_STAGE_PATH]):
            dp = updates.get(UPLOAD_STAGE_PATH)
            if dp is None:
                continue
            val = int(dp.value)
 
            # Rising edge detection: 0 -> 1
            if last_stage_val == 0 and val == 1:
                with upload_stage_lock:
                    upload_stage_active = True
                print("[STAGE] Rising edge detected -> start upload flow")
 
            last_stage_val = val
 
 
def camera_record():
    global upload_stage_active
    writers = {}
    current_session_dir = None
    video_files = {}
    uploaded_folders = load_uploaded_folders()
 
    with VSSClient(KUKSA_HOST, KUKSA_PORT) as client:
        while True:
            any_camera_on = any(status in (1, 2) for status in camera_status.values())
 
            if current_session_dir is None and any_camera_on:
                session_name = datetime.now().strftime(format_video_file)
                current_session_dir = os.path.join(WATCH_FOLDER, session_name)
                session_dir_var["path"] = current_session_dir
                os.makedirs(current_session_dir, exist_ok=True)
                for cam in CAMERA_PATHS:
                    os.makedirs(os.path.join(current_session_dir, cam), exist_ok=True)
 
            if current_session_dir and not any_camera_on:
                # Close writers
                for cam, writer in list(writers.items()):
                    try:
                        writer.release()
                        print(f"[VIDEO] Closed MP4 for {cam}")
                        # start_conversion_async(video_files[cam])  # Start async conversion
                    except Exception as e:
                        print(f"[ERROR] Closing writer for {cam}: {e}")
                writers.clear()
 
                # Wait for all conversions if stage active
                with upload_stage_lock:
                    if upload_stage_active:
                        print("[UPLOAD FLOW] Stage active and camera off. Waiting for conversions...")
                        for future in list(conversion_futures.values()):
                            future.result()  # wait all conversions done
                        conversion_futures.clear()
 
                        folder_path = current_session_dir
                        folder_name = os.path.basename(folder_path)
                        upload_folder(folder_path, folder_name, uploaded_folders, client)
 
                        # Reset stage in KUKSA and locally so it won't retrigger
                        try:
                            client.set_current_values({UPLOAD_STAGE_PATH: Datapoint(0)})
                            print("[STAGE] Reset Upload.Stage to 0")
                        except Exception as e:
                            print(f"[WARN] Failed to reset Upload.Stage: {e}")
 
                        upload_stage_active = False
                        print("[UPLOAD FLOW] Upload completed, stage flag reset.")
 
                video_files.clear()
                current_session_dir = None
                session_dir_var["path"] = None
 
            # Record video frames while camera is on
            for cam, cap in list(active_cameras.items()):
                if camera_status[cam] in (1, 2) and current_session_dir:
                    cam_dir = os.path.join(current_session_dir, cam)
                    if cam not in writers:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        file_path = f'{cam_dir}/{cam}_{timestamp}.webm'
                        writer = cv2.VideoWriter(file_path, fourcc, 30.0, resolution)
                        if writer.isOpened():
                            writers[cam] = writer
                            video_files[cam] = file_path
                            print(f"[VIDEO] Recording MP4: {file_path}")
                        else:
                            print(f"[ERROR] Cannot open writer for {cam}")
                            continue
                    ret, frame = cap.read()
                    if ret:
                        try:
                            frame = cv2.resize(frame, resolution)
                            ts = datetime.now().strftime(f'%Y:%m:%d %H:%M:%S {cam}')
                            size = cv2.getTextSize(ts, cv2.FONT_HERSHEY_COMPLEX, 0.5, 2)[0]
                            cv2.putText(frame, ts, (10, size[1] + 10), cv2.FONT_HERSHEY_COMPLEX, 0.5, (0,), 2)
                            cv2.putText(frame, ts, (10, size[1] + 10), cv2.FONT_HERSHEY_COMPLEX, 0.5, (255, 255, 255), 1)
                            writers[cam].write(frame)
                        except Exception as e:
                            print(f"[ERROR] Writing frame for {cam}: {e}")
                elif cam in writers:
                    try:
                        writers[cam].release()
                        print(f"[VIDEO] Closed MP4 for {cam}")
                        start_conversion_async(video_files[cam])
                        del writers[cam]
                        del video_files[cam]
                    except Exception as e:
                        print(f"[ERROR] Closing writer for {cam}: {e}")
 
            if not stop_queue.empty() and stop_queue.get():
                for cam, writer in list(writers.items()):
                    try:
                        writer.release()
                        print(f"[VIDEO] Closed MP4 for {cam}")
                        start_conversion_async(video_files[cam])
                    except Exception as e:
                        print(f"[ERROR] Closing writer for {cam}: {e}")
                writers.clear()
                video_files.clear()
                for cam in list(active_cameras.keys()):
                    active_cameras[cam].release()
                active_cameras.clear()
                print("[THREAD] Camera record thread exiting.")
                break
 
            time.sleep(0.03)
 
 
# === Main ===
def main():
    monitor_thread = threading.Thread(target=monitor_camera_status_blocking, daemon=True)
    uploader_flag_thread = threading.Thread(target=uploader_thread, daemon=True)
    record_thread = threading.Thread(target=camera_record, daemon=True)
 
    monitor_thread.start()
    uploader_flag_thread.start()
    record_thread.start()
 
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[MAIN] KeyboardInterrupt received, stopping threads...")
        stop_queue.put(True)
        record_thread.join()
        monitor_thread.join()
        uploader_flag_thread.join()
        print("[MAIN] All threads stopped.")
 
 
if __name__ == "__main__":
    main()
