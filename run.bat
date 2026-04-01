@echo off
setlocal
set VENV_DIR=venv

echo == Checking Python ==
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Python not found. Install Python and make sure it is in PATH.
    pause
    exit /b 1
)

:: ─── Virtual Environment ───────────────────────────────────────────────────

if not exist %VENV_DIR%\Scripts\activate.bat (
    echo == Creating venv ==
    python -m venv %VENV_DIR%
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to create venv.
        pause
        exit /b 1
    )
) else (
    echo == venv already exists, skipping ==
)

echo == Activating venv ==
call %VENV_DIR%\Scripts\activate.bat

echo == Upgrading pip ==
python -m pip install --upgrade pip --quiet

:: ─── Python Packages ───────────────────────────────────────────────────────

echo == Checking tqdm ==
python -c "import tqdm" >nul 2>&1
if %errorlevel% neq 0 (
    echo    tqdm not found, installing...
    pip install tqdm --quiet
) else (
    echo    tqdm already installed, skipping.
)

echo == Checking yt-dlp ==
python -c "import yt_dlp" >nul 2>&1
if %errorlevel% neq 0 (
    echo    yt-dlp not found, installing...
    pip install yt-dlp --quiet
) else (
    echo    yt-dlp already installed, skipping.
)

:: ─── ffmpeg (system-level, called via subprocess) ──────────────────────────

echo == Checking ffmpeg ==
where ffmpeg >nul 2>&1
if %errorlevel% neq 0 (
    echo    ffmpeg not found, installing via winget...
    where winget >nul 2>&1
    if %errorlevel% neq 0 (
        echo [ERROR] winget not found. Please install ffmpeg manually:
        echo         https://ffmpeg.org/download.html
        pause
        exit /b 1
    )
    winget install -e --id Gyan.FFmpeg --accept-source-agreements --accept-package-agreements
    if %errorlevel% neq 0 (
        echo [ERROR] ffmpeg installation failed.
        pause
        exit /b 1
    )
    echo    ffmpeg installed. You may need to restart your terminal for PATH to update.
) else (
    echo    ffmpeg already installed, skipping.
)

:: ─── Run ───────────────────────────────────────────────────────────────────

echo.
echo == All checks passed. Running Scraper.py ==
echo.
python Scraper.py

pause