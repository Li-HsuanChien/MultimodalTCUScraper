import os
import sqlite3
import subprocess
import csv
from datetime import date
from pathlib import Path
from tqdm import tqdm
import yt_dlp
import threading

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12"
}


def formatTimeDate(timedate):
    """From DD-MM(TEXT)-YY to DDMMYY"""
    parts = timedate.strip().split("-")
    day = parts[0].zfill(2)
    month_text = parts[1][:3].lower()
    month = MONTH_MAP.get(month_text, "00")
    year = parts[2] if len(parts[2]) == 4 else "20" + parts[2]
    return f"{day}{month}{year}"

def formatTime(t):
    """From HH:MM:SS to seconds as float"""
    h, m, s = t.split(":")
    return round(int(h) * 3600 + int(m) * 60 + float(s), 3)

def getAllTCUs(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            vs.video_urlID,
            vs.meeting_date,
            vs.State,
            vs.County,
            t.video_saved,
            t.audio_saved,
            t.frames_saved,
            t.TCUID,
            t.tcu_start,
            t.tcu_end
        FROM TCU t
        JOIN VideoSegment vs ON t.VIDEOSEGID = vs.ID
        WHERE NOT (t.video_saved = 1 AND t.audio_saved = 1 AND t.frames_saved = 1)
    """)
    rows = cursor.fetchall()
    conn.close()
    return rows

def run_ffmpeg_with_progress(cmd, desc, duration_secs=None):
    """Run an ffmpeg command with a tqdm progress bar, streaming progress via pipe."""
    process = subprocess.Popen(
        cmd + ["-progress", "pipe:1", "-nostats"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,  # capture instead of devnull
        text=True,
    )

    stderr_lines = []
    def consume_stderr():
        for line in process.stderr:
            stderr_lines.append(line)
    stderr_thread = threading.Thread(target=consume_stderr, daemon=True)
    stderr_thread.start()

    with tqdm(total=duration_secs, desc=desc, unit="s", leave=False, dynamic_ncols=True) as pbar:
        last = 0
        for line in process.stdout:
            if line.startswith("out_time_ms="):
                try:
                    ms = int(line.strip().split("=")[1])
                    secs = round(ms / 1_000_000, 2)
                    if secs > last:
                        pbar.update(round(secs - last, 2))
                        last = secs
                except ValueError:
                    pass

    process.wait()
    stderr_thread.join()

    if process.returncode != 0:
        print(f"[ffmpeg stderr]:\n{''.join(stderr_lines)}")
        raise subprocess.CalledProcessError(process.returncode, cmd[0])
    
def downloadVideoSegment(row, DB_PATH):
    video_ID, meeting_date, State, County, TCU_ID, TCU_start_time, TCU_end_time = row
    video_url = f"https://www.youtube.com/watch?v={video_ID}"

    formatted_date = formatTimeDate(meeting_date)
    file_name = f"{State}-{County}-{formatted_date}-{video_ID}-{TCU_ID}"

    output_dir = Path(f"output/video/{State}")
    output_dir.mkdir(parents=True, exist_ok=True)
    video_path = str(output_dir / f"{file_name}.mp4")
    full_path = str(output_dir / f"{video_ID}_full.mp4")

    start_sec = formatTime(TCU_start_time)
    end_sec = formatTime(TCU_end_time)

    # Step 1: Download full video if not already cached
    if not Path(full_path).exists():
        ydl_opts = {
            # "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
            "outtmpl": full_path,
            "quiet": True,
            "no_warnings": True,
            "extractor_args": {
                "youtube": {
                    "js_runtimes": ["node"],
                }
            },
            "remote_components": "ejs:github",
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

        print(f"[downloadVideoSegment] Full video cached: {full_path}")

    # Step 2: Cut the TCU segment with ffmpeg (unchanged)
    run_ffmpeg_with_progress(
        [
            "ffmpeg", "-y",
            "-ss", str(start_sec),
            "-to", str(end_sec),
            "-i", full_path,
            "-c", "copy",
            video_path,
        ],
        desc=f"Cut TCU {TCU_ID}",
        duration_secs=round(end_sec - start_sec, 3),
    )

    print(f"[downloadVideoSegment] Cut segment saved: {video_path}")

    # Step 3: Update DB (unchanged)
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("UPDATE TCU SET video_saved = 1 WHERE TCUID = ?", (TCU_ID,))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"[downloadVideoSegment] DB update failed for TCU {TCU_ID}: {e}")
        raise

    return video_path

def extractAudio(video_path, DB_PATH, tcu_id):
    video_path = Path(video_path)
    file_name = video_path.name  
    audio_file_name = file_name.replace(".mp4", ".wav")

    audio_path = Path("output/audio") / video_path.parts[-2] / audio_file_name
    audio_dir = Path(audio_path).parent
    audio_dir.mkdir(parents=True, exist_ok=True)

    run_ffmpeg_with_progress(
        [
            "ffmpeg", "-y",
            "-i", video_path,
            "-vn",
            "-acodec", "pcm_s16le",
            "-ar", "44100",
            "-ac", "2",
            audio_path,
        ],
        desc=f"Audio  TCU {tcu_id}",
    )

    print(f"[extractAudio] Extracted audio: {audio_path}")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("UPDATE TCU SET audio_saved = 1 WHERE TCUID = ?", (tcu_id,))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"[extractAudio] DB update failed for TCU {tcu_id}: {e}")
        raise

    return audio_path

def extractFrames(video_path, DB_PATH, tcu_id):
    video_path = Path(video_path)
    folder_name = video_path.stem  

    frames_path = Path("output/frames") / video_path.parts[-2] / folder_name / ""
    frames_dir = Path(frames_path).parent
    frames_dir.mkdir(parents=True, exist_ok=True)

    run_ffmpeg_with_progress(
        [
            "ffmpeg", "-y",
            "-i", video_path,
            "-vf", "format=yuvj420p",
            "-vsync", "vfr",
            str(frames_dir / "frame_%d.jpg"),
        ],
        desc=f"Frames TCU {tcu_id}",
    )

    print(f"[extractFrames] Extracted frames to: {frames_dir}")

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("UPDATE TCU SET frames_saved = 1 WHERE TCUID = ?", (tcu_id,))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"[extractFrames] DB update failed for TCU {tcu_id}: {e}")
        raise

    return str(frames_dir)

def exportExtractionMetadata(DB_PATH, output_path):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                t.TCUID,
                vs.video_urlID,
                vs.meeting_date,
                vs.State,
                vs.County,
                t.tcu_start,
                t.tcu_end
            FROM TCU t
            LEFT JOIN VideoSegment vs ON t.VIDEOSEGID = vs.ID
            WHERE t.video_saved = 1
            AND t.audio_saved = 1
            AND t.frames_saved = 1
        """)
        rows = cursor.fetchall()
        conn.close()
    except sqlite3.Error as e:
        print(f"[exportExtractionMetadata] DB query failed: {e}")
        raise

    if not rows:
        print("[exportExtractionMetadata] No fully saved TCUs found.")
        return

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    existing_tcuids = set()
    if output_file.exists():
        try:
            with open(output_file, "r", newline="") as f:
                reader = csv.reader(f)
                next(reader, None)
                for r in reader:
                    if r:
                        existing_tcuids.add(r[0])
        except Exception as e:
            print(f"[exportExtractionMetadata] Failed to read existing CSV: {e}")
            raise

    write_header = not output_file.exists() or output_file.stat().st_size == 0
    new_count = 0
    try:
        with open(output_file, "a", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(["tcu_id", "video_url", "meeting_date", "tcu_start", "tcu_end",
                                "clip_duration_s", "video_clip_path", "audio_clip_path",
                                "frames_folder_path", "extraction_date"])
            for row in rows:
                tcu_id, video_id, meeting_date, State, County, tcu_start, tcu_end = row
                if tcu_id in existing_tcuids:
                    continue
                formatted_date = formatTimeDate(meeting_date)
                file_stem = f"{State}-{County}-{formatted_date}-{video_id}-{tcu_id}"
                writer.writerow([
                    tcu_id,
                    f"https://www.youtube.com/watch?v={video_id}",
                    meeting_date,
                    tcu_start,
                    tcu_end,
                    round(formatTime(tcu_end) - formatTime(tcu_start), 3),
                    f"output/video/{State}/{file_stem}.mp4",
                    f"output/audio/{State}/{file_stem}.wav",
                    f"output/frames/{State}/{file_stem}",
                    date.today().isoformat(),
                ])
                new_count += 1
    except Exception as e:
        print(f"[exportExtractionMetadata] Failed to write CSV: {e}")
        raise

    print(f"[exportExtractionMetadata] Appended {new_count} new TCUs -> {output_file}")


def main():
    DB_PATH = "db/annotation.db"
    rows = getAllTCUs(DB_PATH)
    print(f"[main] Found {len(rows)} TCUs to process.")

    for row in tqdm(rows[:5], desc="Overall TCUs", unit="TCU", dynamic_ncols=True):
        video_ID, meeting_date, State, County, video_saved, audio_saved, frames_saved, TCU_ID, TCU_start_time, TCU_end_time = row

        video_path = None

        if not video_saved:
            try:
                video_path = downloadVideoSegment(
                    [video_ID, meeting_date, State, County, TCU_ID, TCU_start_time, TCU_end_time],
                    DB_PATH
                )
            except Exception as e:
                print(f"[main] Skipping TCU {TCU_ID} — video download failed: {e}")
                continue
        else:
            formatted_date = formatTimeDate(meeting_date)
            file_stem = f"{State}-{County}-{formatted_date}-{video_ID}-{TCU_ID}"
            video_path = f"output/video/{State}/{file_stem}.mp4"

        if not audio_saved:
            try:
                audio_path = extractAudio(video_path, DB_PATH, TCU_ID)
            except Exception as e:
                print(f"[main] Skipping audio for TCU {TCU_ID}: {e}")

        if not frames_saved:
            try:
                frames_dir = extractFrames(video_path, DB_PATH, TCU_ID)
            except Exception as e:
                print(f"[main] Skipping frames for TCU {TCU_ID}: {e}")

    try:
        exportExtractionMetadata(DB_PATH, "output/master_clips.csv")
    except Exception as e:
        print(f"[main] Failed to export metadata: {e}")


if __name__ == "__main__":
    main()