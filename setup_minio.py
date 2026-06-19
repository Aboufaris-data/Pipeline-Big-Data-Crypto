import os
import subprocess
import urllib.request
import sys

# 1. Path Configuration
MINIO_DIR = r"C:\minio"
MINIO_EXE = os.path.join(MINIO_DIR, "minio.exe")
MINIO_DATA = r"C:\minio_data"

# Direct URL for Windows MinIO Binary (108 MiB)
MINIO_URL = "https://dl.min.io/server/minio/release/windows-amd64/minio.RELEASE.2025-09-07T16-13-09Z"

def setup_and_run_minio():
    # Create directories if they do not exist
    if not os.path.exists(MINIO_DIR):
        print(f"[*] Creating application directory: {MINIO_DIR}")
        os.makedirs(MINIO_DIR)
        
    if not os.path.exists(MINIO_DATA):
        print(f"[*] Creating data directory: {MINIO_DATA}")
        os.makedirs(MINIO_DATA)

    # Download MinIO binary if it does not exist
    if not os.path.exists(MINIO_EXE):
        print("[*] Downloading MinIO server (108 MiB)... This may take a moment.")
        try:
            urllib.request.urlretrieve(MINIO_URL, MINIO_EXE)
            print("[+] Download complete. Saved as minio.exe")
        except Exception as e:
            print(f"[-] Download error: {e}")
            sys.exit(1)
    else:
        print("[+] minio.exe already exists.")

    # Run the server
    print("\n[+] Starting MinIO server...")
    print("[!] Warning: Keep this terminal open. Closing it will stop the server.\n")
    
    cmd = [
        MINIO_EXE, "server", MINIO_DATA,
        "--address", ":9000",
        "--console-address", ":9001"  # Fixed WebUI port at 9001
    ]
    
    try:
        # Run server and stream output directly to terminal
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\n[-] MinIO server stopped successfully.")

if __name__ == "__main__":
    setup_and_run_minio()