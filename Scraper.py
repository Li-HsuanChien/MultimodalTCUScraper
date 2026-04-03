import os
import sqlite3
import subprocess
import csv
from datetime import date
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm
import yt_dlp
import threading

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12"
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# DB queries
# ---------------------------------------------------------------------------

def getAllTCUs(DB_PATH):
    """
    Return every TCU that still needs at least one asset saved.
    Groups naturally by VIDEOID (the raw YouTube ID on TCU itself),
    so no JOIN is strictly required for the download step — but we
    still pull VideoSegment fields needed for file naming.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            t.VIDEOID,
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


def groupTCUsByVideo(rows):
    """
    Group query rows by VIDEOID.

    Returns:
        dict[video_id -> list[row_dict]]

    Each row_dict has all the fields needed for cutting + naming.
    The first row for a video also supplies meeting_date / State / County
    (they're the same for every TCU that shares a video_urlID).
    """
    grouped = defaultdict(list)
    for row in rows:
        video_id, meeting_date, state, county, \
            video_saved, audio_saved, frames_saved, \
            tcu_id, tcu_start, tcu_end = row

        grouped[video_id].append({
            "video_id":     video_id,
            "meeting_date": meeting_date,
            "state":        state,
            "county":       county,
            "video_saved":  video_saved,
            "audio_saved":  audio_saved,
            "frames_saved": frames_saved,
            "tcu_id":       tcu_id,
            "tcu_start":    tcu_start,
            "tcu_end":      tcu_end,
        })
    return grouped


# ---------------------------------------------------------------------------
# ffmpeg helper
# ---------------------------------------------------------------------------

def run_ffmpeg_with_progress(cmd, desc, duration_secs=None):
    """Run an ffmpeg command with a tqdm progress bar."""
    process = subprocess.Popen(
        cmd + ["-progress", "pipe:1", "-nostats"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stderr_lines = []

    def consume_stderr():
        for line in process.stderr:
            stderr_lines.append(line)

    stderr_thread = threading.Thread(target=consume_stderr, daemon=True)
    stderr_thread.start()

    with tqdm(total=duration_secs, desc=desc, unit="s",
              leave=False, dynamic_ncols=True) as pbar:
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


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def downloadFullVideo(video_id, state, DB_PATH):
    """
    Download the full YouTube video once and cache it.
    Returns the local path to the cached file.
    """
    output_dir = Path(f"output/video/{state}")
    output_dir.mkdir(parents=True, exist_ok=True)
    full_path = output_dir / f"{video_id}_full.mp4"

    if full_path.exists():
        print(f"[downloadFullVideo] Already cached: {full_path}")
        return str(full_path)

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    ydl_opts = {
        "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
        "outtmpl": str(full_path),
        "quiet": False,
        "no_warnings": False,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

    print(f"[downloadFullVideo] Downloaded: {full_path}")
    return str(full_path)


# ---------------------------------------------------------------------------
# Batch cutting — one ffmpeg call per TCU (stream-copy is fast)
# ---------------------------------------------------------------------------

def cutTCUSegments(full_path, tcus, DB_PATH):
    """
    Cut every TCU segment from *full_path* that still needs its video saved.

    Args:
        full_path: path to the cached full video file
        tcus:      list of row_dicts for this video
        DB_PATH:   sqlite db path

    Returns:
        dict[tcu_id -> video_clip_path]  (only for successfully cut TCUs)
    """
    results = {}

    needs_cut = [t for t in tcus if not t["video_saved"]]
    if not needs_cut:
        # Reconstruct paths for already-saved TCUs so callers can use them
        for t in tcus:
            formatted_date = formatTimeDate(t["meeting_date"])
            file_stem = f"{t['state']}-{t['county']}-{formatted_date}-{t['video_id']}-{t['tcu_id']}"
            results[t["tcu_id"]] = f"output/video/{t['state']}/{file_stem}.mp4"
        return results

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for t in tqdm(needs_cut, desc=f"  Cutting {Path(full_path).stem}", unit="TCU",
                  leave=False, dynamic_ncols=True):
        formatted_date = formatTimeDate(t["meeting_date"])
        file_stem = f"{t['state']}-{t['county']}-{formatted_date}-{t['video_id']}-{t['tcu_id']}"
        output_dir = Path(f"output/video/{t['state']}")
        output_dir.mkdir(parents=True, exist_ok=True)
        video_path = str(output_dir / f"{file_stem}.mp4")

        start_sec = formatTime(t["tcu_start"])
        end_sec   = formatTime(t["tcu_end"])

        try:
            run_ffmpeg_with_progress(
                [
                    "ffmpeg", "-y",
                    "-ss", str(start_sec),
                    "-to", str(end_sec),
                    "-i", full_path,
                    "-c", "copy",
                    video_path,
                ],
                desc=f"Cut {t['tcu_id']}",
                duration_secs=round(end_sec - start_sec, 3),
            )
            cursor.execute("UPDATE TCU SET video_saved = 1 WHERE TCUID = ?", (t["tcu_id"],))
            conn.commit()
            results[t["tcu_id"]] = video_path
            print(f"[cutTCUSegments] Saved: {video_path}")
        except Exception as e:
            print(f"[cutTCUSegments] Failed to cut TCU {t['tcu_id']}: {e}")

    conn.close()

    # Also map already-saved TCUs to their expected paths
    for t in tcus:
        if t["tcu_id"] not in results:
            formatted_date = formatTimeDate(t["meeting_date"])
            file_stem = f"{t['state']}-{t['county']}-{formatted_date}-{t['video_id']}-{t['tcu_id']}"
            results[t["tcu_id"]] = f"output/video/{t['state']}/{file_stem}.mp4"

    return results


# ---------------------------------------------------------------------------
# Audio / Frames  (unchanged logic, just cleaner signatures)
# ---------------------------------------------------------------------------

def extractAudio(video_path, tcu_id, DB_PATH):
    video_path = Path(video_path)
    audio_file_name = video_path.stem + ".wav"
    audio_dir = Path("output/audio") / video_path.parts[-2]
    audio_dir.mkdir(parents=True, exist_ok=True)
    audio_path = audio_dir / audio_file_name

    run_ffmpeg_with_progress(
        [
            "ffmpeg", "-y",
            "-i", str(video_path),
            "-vn",
            "-acodec", "pcm_s16le",
            "-ar", "44100",
            "-ac", "2",
            str(audio_path),
        ],
        desc=f"Audio  {tcu_id}",
    )
    print(f"[extractAudio] {audio_path}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE TCU SET audio_saved = 1 WHERE TCUID = ?", (tcu_id,))
    conn.commit()
    conn.close()
    return str(audio_path)


def extractFrames(video_path, tcu_id, DB_PATH):
    video_path = Path(video_path)
    frames_dir = Path("output/frames") / video_path.parts[-2] / video_path.stem
    frames_dir.mkdir(parents=True, exist_ok=True)

    run_ffmpeg_with_progress(
        [
            "ffmpeg", "-y",
            "-i", str(video_path),
            "-vf", "format=yuvj420p",
            "-vsync", "vfr",
            str(frames_dir / "frame_%d.jpg"),
        ],
        desc=f"Frames {tcu_id}",
    )
    print(f"[extractFrames] {frames_dir}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE TCU SET frames_saved = 1 WHERE TCUID = ?", (tcu_id,))
    conn.commit()
    conn.close()
    return str(frames_dir)


# ---------------------------------------------------------------------------
# Metadata export  (unchanged)
# ---------------------------------------------------------------------------

def exportExtractionMetadata(DB_PATH, output_path):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            t.TCUID,
            t.VIDEOID,
            vs.meeting_date,
            vs.State,
            vs.County,
            t.tcu_start,
            t.tcu_end
        FROM TCU t
        LEFT JOIN VideoSegment vs ON t.VIDEOSEGID = vs.ID
        WHERE t.video_saved = 1 AND t.audio_saved = 1 AND t.frames_saved = 1
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("[exportExtractionMetadata] No fully saved TCUs found.")
        return

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    existing_tcuids = set()
    if output_file.exists() and output_file.stat().st_size > 0:
        with open(output_file, "r", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            existing_tcuids = {r[0] for r in reader if r}

    write_header = not output_file.exists() or output_file.stat().st_size == 0
    new_count = 0
    with open(output_file, "a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "tcu_id", "video_url", "meeting_date", "tcu_start", "tcu_end",
                "clip_duration_s", "video_clip_path", "audio_clip_path",
                "frames_folder_path", "extraction_date",
            ])
        for tcu_id, video_id, meeting_date, state, county, tcu_start, tcu_end in rows:
            if tcu_id in existing_tcuids:
                continue
            formatted_date = formatTimeDate(meeting_date)
            file_stem = f"{state}-{county}-{formatted_date}-{video_id}-{tcu_id}"
            writer.writerow([
                tcu_id,
                f"https://www.youtube.com/watch?v={video_id}",
                meeting_date,
                tcu_start,
                tcu_end,
                round(formatTime(tcu_end) - formatTime(tcu_start), 3),
                f"output/video/{state}/{file_stem}.mp4",
                f"output/audio/{state}/{file_stem}.wav",
                f"output/frames/{state}/{file_stem}",
                date.today().isoformat(),
            ])
            new_count += 1

    print(f"[exportExtractionMetadata] Appended {new_count} new TCUs -> {output_file}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    DB_PATH = "db/annotation.db"

    rows = getAllTCUs(DB_PATH)
    print(f"[main] Found {len(rows)} TCUs to process.")

    grouped = groupTCUsByVideo(rows)
    print(f"[main] Grouped into {len(grouped)} unique videos.")

    for video_id, tcus in tqdm(grouped.items(), desc="Videos", unit="video", dynamic_ncols=True):
        # All TCUs for the same video share state/county/meeting_date
        state   = tcus[0]["state"]

        # ── 1. Download full video once ──────────────────────────────────
        try:
            full_path = downloadFullVideo(video_id, state, DB_PATH)
        except Exception as e:
            print(f"[main] Skipping video {video_id} — download failed: {e}")
            continue

        # ── 2. Batch-cut all TCU segments from that video ────────────────
        try:
            clip_paths = cutTCUSegments(full_path, tcus, DB_PATH)
        except Exception as e:
            print(f"[main] Skipping cuts for video {video_id}: {e}")
            continue

        # ── 3. Extract audio + frames per TCU ───────────────────────────
        for t in tcus:
            tcu_id    = t["tcu_id"]
            clip_path = clip_paths.get(tcu_id)
            if not clip_path or not Path(clip_path).exists():
                print(f"[main] No clip found for TCU {tcu_id}, skipping audio/frames.")
                continue

            if not t["audio_saved"]:
                try:
                    extractAudio(clip_path, tcu_id, DB_PATH)
                except Exception as e:
                    print(f"[main] Audio failed for TCU {tcu_id}: {e}")

            if not t["frames_saved"]:
                try:
                    extractFrames(clip_path, tcu_id, DB_PATH)
                except Exception as e:
                    print(f"[main] Frames failed for TCU {tcu_id}: {e}")

    # ── 4. Export metadata CSV ───────────────────────────────────────────
    try:
        exportExtractionMetadata(DB_PATH, "output/master_clips.csv")
    except Exception as e:
        print(f"[main] Failed to export metadata: {e}")


if __name__ == "__main__":
    main()