#!/bin/bash
set -e

VENV_DIR="venv"

echo "== Checking venv =="
if [ ! -d "$VENV_DIR" ]; then
    echo "venv not found, creating..."
    python3 -m venv $VENV_DIR
fi

echo "== Activating venv =="
source $VENV_DIR/bin/activate

echo "== Upgrading pip =="
pip install --upgrade pip

echo "== Checking yt-dlp =="
python -c "import yt_dlp" 2>/dev/null || pip install yt-dlp


echo "== Installing requirements =="
pip install -r requirements.txt

echo "== Checking ffmpeg =="
if ! command -v ffmpeg >/dev/null 2>&1; then
    echo "ffmpeg not found, installing..."

    if command -v apt >/dev/null 2>&1; then
        module load ffmpeg
    else
        echo "No supported package manager found."
        echo "Please install ffmpeg manually, then re-run."
        exit 1
    fi
else
    echo "ffmpeg already installed."
fi

echo "== Running Scraper.py =="
python Scraper.py