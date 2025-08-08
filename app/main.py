import os
import cv2
import yaml
import json
import time
import queue
import threading
import mimetypes
import requests
import urllib3
from datetime import datetime
from kuksa_client.grpc import VSSClient, Datapoint
import paho.mqtt.client as mqtt

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === Load config ===
with open('config/record.yaml', 'r') as f:
    record_config = yaml.safe_load(f)

WATCH_FOLDER = record_config['base_directory']
VIDEO_EXTENSIONS = ('.mp4', '.webm', '.ogg')
UPLOAD_STAGE_PATH = "Vehicle.Connectivity.Emergency.Upload.Stage"
UPLOAD_STATUS_PATH = "Vehicle.Connectivity.Emergency.Upload.Status"
MAX_RETRIES = 3
SERVER_UPLOAD_URL = 'https://34.127.65.112:8000/upload'
UPLOADED_TRACK_FILE = 'uploaded_folders.json'

KUKSA_HOST = "HnR"
KUKSA_PORT = 55555

MQTT_BROKER = "34.127.65.112"
MQTT_PORT = 1883
MQTT_TOPIC = "time_crash"

camera_index = record_config['camera_index']
format_video_file = record_config['format_video']
height = record_config['resolution']['height']
width = record_config['resolution']['width']
threshold = record_config['maximum_video_time']  # seconds
resolution = (width, height)
fourcc = cv2.VideoWriter_fourcc(*'mp4v')

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
active_cameras = {}                 # cam_name -> cv2.VideoCapture
camera_status = {cam: 0 for cam in CAMERA_PATHS}  # 0=off,1=view,2=record
session_dir_var = {"path": None}    # current session folder path (shared)

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
    """Publish MQTT timestamp (this is called once per upload-session on stage=1)."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        mqtt_client.publish(MQTT_TOPIC, f"Video uploaded at {timestamp}")
        print(f"[MQTT] Published timestamp: {timestamp}")
    except Exception as e:
        print(f"[ERROR] MQTT publish failed: {e}")

def get_videos_in_folder(folder_path):
    videos = []
    if not folder_path or not os.path.isdir(folder_path):
        return videos
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith(VIDEO_EXTENSIONS):
                videos.append((file, os.path.join(root, file)))
    return sorted(videos, key=lambda x: os.path.getmtime(x[1]))

def check_and_upload_remaining(folder_path, folder_name, already_uploaded_set, client):
    """
    After cameras off: upload any remaining files regardless of duration.
    already_uploaded_set contains full paths that have been already uploaded in this session.
    """
    videos = get_videos_in_folder(folder_path)
    remaining_videos = [v for v in videos if v[1] not in already_uploaded_set]
    if not remaining_videos:
        print("[REUPLOAD CHECK] No remaining files to upload.")
        return

    print(f"[REUPLOAD CHECK] Uploading remaining {len(remaining_videos)} files in {folder_name}")
    for video_name, full_path in remaining_videos:
        retries = 0
        success = False
        while retries < MAX_RETRIES and not success:
            try:
                with open(full_path, 'rb') as f:
                    mime_type, _ = mimetypes.guess_type(video_name)
                    mime_type = mime_type or 'application/octet-stream'
                    files = {'video': (video_name, f, mime_type)}
                    response = requests.post(SERVER_UPLOAD_URL, files=files, verify=False)
                if response.status_code == 200:
                    print(f"[UPLOAD SUCCESS] (Delayed) {video_name}")
                    already_uploaded_set.add(full_path)
                    success = True
                else:
                    print(f"[UPLOAD FAILED] (Delayed) {video_name} - Status: {response.status_code}")
            except Exception as e:
                print(f"[ERROR] Delayed upload error with {video_name}: {e}")
            retries += 1

def upload_folder(folder_path, folder_name, uploaded_folders, client):
    """
    Upload files in folder_path that are at least `threshold` seconds long.
    After initial pass we mark folder as uploaded (so it won't be re-uploaded later as a full session),
    but we still check for remaining short files after cameras off (check_and_upload_remaining).
    """
    if folder_name in uploaded_folders:
        print(f"[SKIP] Folder {folder_name} already uploaded.")
        return

    print(f"[UPLOAD] Uploading folder: {folder_name}")
    videos = get_videos_in_folder(folder_path)
    uploaded = False
    uploaded_file_set = set()  # track uploaded files in this session

    try:
        # indicate uploading
        client.set_current_values({UPLOAD_STATUS_PATH: Datapoint(2)})

        for video_name, full_path in videos:
            # === Check duration and only upload if >= threshold (with small margin) ===
            cap = cv2.VideoCapture(full_path)
            if not cap.isOpened():
                print(f"[SKIP] Cannot open {video_name} to check duration - will retry later")
                cap.release()
                continue

            fps = cap.get(cv2.CAP_PROP_FPS) or 0
            frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0
            cap.release()

            # compute duration robustly; if fps missing, skip this file for initial upload
            duration = (frame_count / fps) if (fps > 0 and frame_count > 0) else 0

            if duration < threshold - 1:  # allow 1s margin
                print(f"[SKIP] {video_name} too short ({duration:.2f}s < {threshold}s), will retry later")
                continue

            # Try upload (with retries)
            retries = 0
            success = False
            while retries < MAX_RETRIES and not success:
                try:
                    with open(full_path, 'rb') as f:
                        mime_type, _ = mimetypes.guess_type(video_name)
                        mime_type = mime_type or 'application/octet-stream'
                        files = {'video': (video_name, f, mime_type)}
                        response = requests.post(SERVER_UPLOAD_URL, files=files, verify=False)
                    if response.status_code == 200:
                        print(f"[UPLOAD SUCCESS] {video_name}")
                        uploaded_file_set.add(full_path)
                        success = True
                        uploaded = True
                    else:
                        print(f"[UPLOAD FAILED] {video_name} - Status: {response.status_code}")
                except Exception as e:
                    print(f"[ERROR] Upload error with {video_name}: {e}")
                retries += 1

        # done initial upload pass
        client.set_current_values({UPLOAD_STATUS_PATH: Datapoint(0)})
        if uploaded:
            uploaded_folders.add(folder_name)
            save_uploaded_folders(uploaded_folders)
            print(f"[DONE] Upload complete for {folder_name} (initial pass)")

        # wait briefly for cameras to turn off, then upload remaining regardless of duration
        print("[WAIT] Waiting for all cameras to turn off to check remaining files...")
        timeout = time.time() + 60
        while time.time() < timeout:
            all_off = all(camera_status[cam] == 0 for cam in CAMERA_PATHS)
            if all_off:
                check_and_upload_remaining(folder_path, folder_name, uploaded_file_set, client)
                break
            time.sleep(1)

    except Exception as e:
        print(f"[ERROR] Upload process failed: {e}")

# === Uploader thread: LISTEN for Upload.Stage and trigger ===
def uploader_thread():
    uploaded_folders = load_uploaded_folders()
    last_sent_session = None  # track to send MQTT once per session (session = folder path at time of stage=1)

    with VSSClient(KUKSA_HOST, KUKSA_PORT) as client:
        for updates in client.subscribe_current_values([UPLOAD_STAGE_PATH]):
            datapoint = updates.get(UPLOAD_STAGE_PATH)
            if datapoint is not None:
                print("[SUBSCRIBE] Received Upload.Stage datapoint:", datapoint.value)
            if datapoint and datapoint.value == 1:
                # Determine session id (folder path or generated id if None)
                current_session = session_dir_var.get("path")
                session_id = current_session if current_session else f"no_session_{datetime.now().isoformat()}"

                # Send MQTT/email once per session when stage=1 arrives
                if session_id != last_sent_session:
                    publish_timestamp()
                    last_sent_session = session_id

                # Trigger upload for current session folder (if exists)
                folder_path = session_dir_var.get("path")
                if folder_path:
                    folder_name = os.path.basename(folder_path)
                    print(f"[TRIGGER] Upload signal received for folder {folder_name}")
                    upload_folder(folder_path, folder_name, uploaded_folders, client)
                else:
                    print("[WARN] No active recording folder to upload (but notification already sent).")

# === Camera monitoring and recording ===
def monitor_camera_status_blocking():
    """
    Subscribe to KUKSA camera activation paths and open/close VideoCapture accordingly.
    Uses CV_CAP_V4L2 on Linux (cv2.CAP_V4L2). If that fails, try default backend.
    """
    with VSSClient(KUKSA_HOST, KUKSA_PORT) as client:
        for updates in client.subscribe_current_values(CAMERA_PATHS.values()):
            for path, datapoint in updates.items():
                if datapoint is None:
                    continue
                cam = [k for k, v in CAMERA_PATHS.items() if v == path][0]
                new_status = int(datapoint.value)
                print(f"[KUKSA] {cam} status -> {new_status}")
                if camera_index.get(cam, 10) == 10:
                    print(f"[KUKSA] {cam} disabled via config (index=10)")
                    continue

                if new_status == 0 and camera_status[cam] in (1, 2):
                    # release camera
                    if cam in active_cameras:
                        active_cameras[cam].release()
                        del active_cameras[cam]
                        print(f"[CAM] Released {cam}")
                    camera_status[cam] = 0
                elif new_status in (1, 2):
                    if cam not in active_cameras:
                        # try V4L2 backend on Linux
                        try:
                            cap = cv2.VideoCapture(camera_index[cam], cv2.CAP_V4L2)
                            time.sleep(2)
                        except Exception:
                            cap = cv2.VideoCapture(camera_index[cam])

                        if cap.isOpened():
                            # set resolution; try to set FPS too (may be ignored)
                            cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
                            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
                            try:
                                cap.set(cv2.CAP_PROP_FPS, 30)
                            except Exception:
                                pass
                            active_cameras[cam] = cap
                            camera_status[cam] = new_status
                            print(f"[CAM] Opened {cam} (index={camera_index[cam]})")
                        else:
                            print(f"[ERROR] Cannot open {cam} (index={camera_index[cam]})")
                            camera_status[cam] = 0

def save_and_create_writer(cam, writers, directory_name, file_index):
    """Close existing writer and create new writer file"""
    if cam in writers and writers[cam] is not None:
        writers[cam].release()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = f'{directory_name}/{cam}_{file_index}_{timestamp}.mp4'
    writer = cv2.VideoWriter(file_path, fourcc, 30.0, resolution)
    writers[cam] = writer
    print(f"[WRITE] New file for {cam}: {file_path}")
    return file_path

def camera_record():
    """
    Main recording loop:
    - use frame counting with camera-reported fps (fallback to 30) to split files at `threshold` seconds
    - write frames to per-camera writers
    """
    writers = {}         # cam -> VideoWriter
    frame_counts = {}    # cam -> frames written in current segment
    file_indices = {}    # cam -> index integer
    current_session_dir = None

    while True:
        any_camera_on = any(status in (1, 2) for status in camera_status.values())
        if current_session_dir is None and any_camera_on:
            session_name = datetime.now().strftime(format_video_file)
            current_session_dir = os.path.join(WATCH_FOLDER, session_name)
            session_dir_var["path"] = current_session_dir
            os.makedirs(current_session_dir, exist_ok=True)
            for cam in CAMERA_PATHS:
                os.makedirs(os.path.join(current_session_dir, cam), exist_ok=True)
            print(f"[DIR] Created session dir: {current_session_dir}")

        if current_session_dir and not any_camera_on:
            # close writers and reset state
            for w in writers.values():
                w.release()
            writers.clear()
            frame_counts.clear()
            file_indices.clear()
            print(f"[DIR] Session closed: {current_session_dir}")
            current_session_dir = None
            session_dir_var["path"] = None

        # record frames for active cameras
        for cam, cap in list(active_cameras.items()):
            if camera_status.get(cam) in (1, 2) and current_session_dir:
                cam_dir = os.path.join(current_session_dir, cam)
                if cam not in writers:
                    file_indices[cam] = 0
                    frame_counts[cam] = 0
                    save_and_create_writer(cam, writers, cam_dir, file_indices[cam])
                ret, frame = cap.read()
                if not ret:
                    # skip if no frame
                    time.sleep(0.01)
                    continue

                # resize & annotate
                frame_save = cv2.resize(frame, resolution)
                timestamp_text = datetime.now().strftime(f'%Y:%m:%d %H:%M:%S {cam}')
                size = cv2.getTextSize(timestamp_text, cv2.FONT_HERSHEY_COMPLEX, 0.5, 2)[0]
                cv2.putText(frame_save, timestamp_text, (10, size[1] + 10),
                            cv2.FONT_HERSHEY_COMPLEX, 0.5, (0,), 2, cv2.LINE_AA)
                cv2.putText(frame_save, timestamp_text, (10, size[1] + 10),
                            cv2.FONT_HERSHEY_COMPLEX, 0.5, (255, 255, 255), 1, cv2.LINE_AA)

                # write and count frame
                writers[cam].write(frame_save)
                frame_counts[cam] = frame_counts.get(cam, 0) + 1

                # determine fps for this cap (fallback 30)
                fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
                frames_per_clip = int(max(1, round(threshold * fps)))

                # if we've written enough frames, rotate file
                if frame_counts[cam] >= frames_per_clip:
                    writers[cam].release()
                    file_indices[cam] += 1
                    frame_counts[cam] = 0
                    save_and_create_writer(cam, writers, cam_dir, file_indices[cam])
            else:
                # camera not active: ensure writer cleanup if exists
                if cam in writers:
                    writers[cam].release()
                    del writers[cam]
                    frame_counts.pop(cam, None)
                    file_indices.pop(cam, None)

        # graceful stop check
        if not stop_queue.empty():
            stop = stop_queue.get()
            if stop:
                for w in writers.values():
                    w.release()
                writers.clear()
                for cam in list(active_cameras.keys()):
                    active_cameras[cam].release()
                active_cameras.clear()
                print("[REC] Stop received, exiting record loop.")
                break

        # sleep small amount - keep loop responsive
        time.sleep(1 / 30.0)

# === Main ===
def main():
    os.makedirs(WATCH_FOLDER, exist_ok=True)
    threading.Thread(target=monitor_camera_status_blocking, daemon=True).start()
    threading.Thread(target=camera_record, daemon=True).start()
    threading.Thread(target=uploader_thread, daemon=True).start()
    print("[MAIN] Started camera, recorder and uploader threads.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[MAIN] KeyboardInterrupt, shutting down.")
        stop_queue.put(True)
        time.sleep(1)

if __name__ == "__main__":
    main()
