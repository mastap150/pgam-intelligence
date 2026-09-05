"""
video/render.py — Video Rendering Pipeline (spec §11).

script → timeline (data) → FFmpeg → 1080×1920 H.264/AAC.

The timeline is a plain dict, deliberately renderer-agnostic: a Remotion
renderer can consume the same structure later. This FFmpeg implementation:

  * trims/scales/crops each matched asset to its shot window (9:16 cover)
  * fills unmatched shots with a brand-colored placeholder (lavfi) so a
    preview render always completes — QA flags placeholders so they cannot
    ship (§15 duplicate/missing visuals)
  * burns captions via libass, styled from the central theme (§10)
  * draws brand components (location label, CTA, fare card text, source
    label) as theme-driven drawtext overlays; logo overlay when the logo
    file exists
  * appends the end card, mixes VO over ducked music at theme loudness

If ffmpeg is missing the render job records status=blocked rather than
raising — the scheduler must survive (repo convention).
"""

import json
import shutil
import subprocess
from pathlib import Path

from video import settings, theme
from video.models import new_id
from video.store import store

FFMPEG = shutil.which("ffmpeg")


# ---------------------------------------------------------------------------
# Timeline assembly
# ---------------------------------------------------------------------------

def build_timeline(video: dict, script: dict, asset_matches: list[dict],
                   vo_path: str | None, music_path: str | None = None) -> dict:
    t = theme.theme()
    lay = t["layout"]
    s = store()
    match_by_seq = {m["shot_seq"]: m["asset_id"] for m in asset_matches}

    segments = []
    for shot in script.get("shot_list", []):
        asset_id = match_by_seq.get(shot.get("seq"))
        asset = s.get("assets", asset_id) if asset_id else None
        segments.append({
            "start": float(shot.get("start", 0)),
            "end": float(shot.get("end", 0)),
            "asset_path": (asset or {}).get("file_url", ""),
            "placeholder": not asset,
            "onscreen_text": shot.get("onscreen_text", ""),
            "brand_component": shot.get("brand_component", ""),
        })

    duration = max((seg["end"] for seg in segments), default=0.0)
    end_card = theme.component("end_card")
    return {
        "width": lay["width"], "height": lay["height"],
        "duration": duration,
        "segments": segments,
        "captions": script.get("captions", []),
        "vo_path": vo_path or "",
        "music_path": music_path or "",
        "end_card_seconds": float(end_card.get("duration_seconds", 2.0)),
        "location_label": video.get("destination", ""),
        "cta_text": script.get("cta", ""),
        "source_label": theme.component("article_source_label").get("prefix", ""),
    }


# ---------------------------------------------------------------------------
# Captions (.ass styled from theme)
# ---------------------------------------------------------------------------

def _ass_time(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int(seconds % 3600 // 60)
    s = seconds % 60
    return f"{h}:{m:02d}:{s:05.2f}"


def _hex_to_ass(hex_color: str) -> str:
    """#RRGGBB[AA] -> &HAABBGGRR (ASS is little-endian BGR with alpha)."""
    hex_color = hex_color.lstrip("#")
    r, g, b = hex_color[0:2], hex_color[2:4], hex_color[4:6]
    alpha_hex = hex_color[6:8] if len(hex_color) == 8 else "00"
    # ASS alpha: 00 opaque, FF transparent — invert the CSS-style alpha.
    a = 255 - int(alpha_hex, 16) if len(hex_color) == 8 else 0
    return f"&H{a:02X}{b}{g}{r}".upper()


def write_ass_captions(captions: list[dict], out_path: Path) -> Path:
    t = theme.theme()
    typo, lay = t["typography"], t["layout"]
    header = f"""[Script Info]
PlayResX: {lay['width']}
PlayResY: {lay['height']}
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, BackColour, Bold, Alignment, MarginL, MarginR, MarginV, BorderStyle, Outline, Shadow
Style: Caption,{typo['text']},{typo['caption_size_px']},{_hex_to_ass(t['colors']['caption_text'])},{_hex_to_ass(t['colors']['caption_bg'])},1,2,{lay['safe_margin_px']},{lay['safe_margin_px']},{lay['caption_baseline_from_bottom_px']},4,0,0

[Events]
Format: Layer, Start, End, Style, Text
"""
    lines = [
        f"Dialogue: 0,{_ass_time(c['start'])},{_ass_time(c['end'])},Caption,{c['text']}"
        for c in captions
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(header + "\n".join(lines) + "\n")
    return out_path


# ---------------------------------------------------------------------------
# FFmpeg command
# ---------------------------------------------------------------------------

def _drawtext(text: str, size: int, color: str, x: str, y: str,
              box_color: str | None = None) -> str:
    safe = text.replace("\\", "").replace("'", "’").replace(":", "\\:").replace(",", "\\,")
    parts = [f"drawtext=text='{safe}'", f"fontsize={size}", f"fontcolor={color}",
             f"x={x}", f"y={y}"]
    if box_color:
        parts += ["box=1", f"boxcolor={box_color}", "boxborderw=24"]
    return ":".join(parts)


def build_ffmpeg_command(timeline: dict, ass_path: Path | None, out_path: Path) -> list[str]:
    w, h = timeline["width"], timeline["height"]
    t = theme.theme()
    inputs: list[str] = []
    filters: list[str] = []
    n = 0

    for seg in timeline["segments"]:
        dur = max(0.1, seg["end"] - seg["start"])
        if seg["placeholder"] or not seg["asset_path"] or not Path(seg["asset_path"]).exists():
            inputs += ["-f", "lavfi", "-t", f"{dur:.2f}",
                       "-i", f"color=c={t['colors']['ink']}:s={w}x{h}:r=30"]
            filters.append(f"[{n}:v]format=yuv420p,setsar=1[v{n}]")
        else:
            inputs += ["-i", seg["asset_path"]]
            filters.append(
                f"[{n}:v]trim=duration={dur:.2f},setpts=PTS-STARTPTS,"
                f"scale={w}:{h}:force_original_aspect_ratio=increase,"
                f"crop={w}:{h},fps=30,format=yuv420p,setsar=1[v{n}]")
        n += 1

    # End card.
    end_s = timeline["end_card_seconds"]
    inputs += ["-f", "lavfi", "-t", f"{end_s:.2f}",
               "-i", f"color=c={theme.component('end_card').get('bg', '#101418')}:s={w}x{h}:r=30"]
    filters.append(f"[{n}:v]format=yuv420p,setsar=1[v{n}]")
    n += 1

    concat_in = "".join(f"[v{i}]" for i in range(n))
    filters.append(f"{concat_in}concat=n={n}:v=1:a=0[vc]")

    # Brand overlays (theme-driven).
    overlays = []
    lay = t["layout"]
    typo = t["typography"]
    if timeline["location_label"]:
        overlays.append(_drawtext(timeline["location_label"].title(), typo["label_size_px"],
                                  "white", str(lay["safe_margin_px"]), str(lay["safe_margin_px"] + 40),
                                  box_color="black@0.55"))
    if timeline["source_label"]:
        overlays.append(_drawtext(timeline["source_label"],
                                  theme.component("article_source_label").get("size_px", 34),
                                  "white@0.85", "(w-text_w)/2", f"h-{lay['safe_margin_px']}"))
    chain = "[vc]"
    for i, ov in enumerate(overlays):
        out_lbl = f"[vo{i}]"
        filters.append(f"{chain}{ov}{out_lbl}")
        chain = out_lbl
    if ass_path:
        filters.append(f"{chain}ass={ass_path}[vfinal]")
        chain = "[vfinal]"

    cmd = [FFMPEG or "ffmpeg", "-y", *inputs]

    # Audio: VO over ducked music, else VO alone, else silence.
    audio_cfg = t["audio"]
    vo, music = timeline["vo_path"], timeline["music_path"]
    a_inputs = 0
    if vo and Path(vo).exists():
        cmd += ["-i", vo]
        vo_idx = n + a_inputs
        a_inputs += 1
    else:
        vo_idx = None
    if music and Path(music).exists():
        cmd += ["-i", music]
        music_idx = n + a_inputs
        a_inputs += 1
    else:
        music_idx = None

    if vo_idx is not None and music_idx is not None:
        filters.append(
            f"[{music_idx}:a]volume={audio_cfg['music_duck_db']}dB[am];"
            f"[{vo_idx}:a][am]amix=inputs=2:duration=first:dropout_transition=2,"
            f"loudnorm=I={audio_cfg['voice_lufs']}[aout]")
        a_map = "[aout]"
    elif vo_idx is not None:
        filters.append(f"[{vo_idx}:a]loudnorm=I={audio_cfg['voice_lufs']}[aout]")
        a_map = "[aout]"
    elif music_idx is not None:
        filters.append(f"[{music_idx}:a]loudnorm=I={audio_cfg['music_lufs']}[aout]")
        a_map = "[aout]"
    else:
        cmd += ["-f", "lavfi", "-t", f"{timeline['duration'] + end_s:.2f}",
                "-i", "anullsrc=channel_layout=stereo:sample_rate=48000"]
        a_map = f"{n + a_inputs}:a"

    cmd += [
        "-filter_complex", ";".join(filters),
        "-map", chain, "-map", a_map,
        "-t", f"{timeline['duration'] + end_s:.2f}",
        "-c:v", "libx264", "-profile:v", "high", "-crf", "21",
        "-maxrate", "12M", "-bufsize", "24M", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
        "-movflags", "+faststart",
        str(out_path),
    ]
    return cmd


# ---------------------------------------------------------------------------
# Render job
# ---------------------------------------------------------------------------

def render(video_id: str, music_path: str | None = None) -> dict:
    """Render a video from its stored script/assets/VO. Returns the render
    job record; on success the video record carries file_path + duration."""
    s = store()
    video = s.get("videos", video_id)
    if not video:
        raise KeyError(video_id)
    script = s.get("scripts", video.get("script_id", "")) or {}

    job = {"id": new_id("rj"), "video_id": video_id, "status": "queued",
           "created_by": "dve", "log": []}
    s.put("render_jobs", job)

    if not FFMPEG:
        job["status"] = "blocked"
        job["log"].append("ffmpeg not found on PATH")
        settings.log("render", f"BLOCKED {video_id}: ffmpeg missing")
        return s.put("render_jobs", job)

    out_dir = settings.DATA_DIR / "renders"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{video_id}.mp4"
    ass_path = out_dir / f"{video_id}.ass"

    asset_matches = [{"shot_seq": sh.get("seq"), "asset_id": aid}
                     for sh, aid in zip(script.get("shot_list", []),
                                        video.get("asset_ids", []))]
    vo_path = (video.get("vo_path") or "")
    timeline = build_timeline(video, script, asset_matches, vo_path, music_path)
    write_ass_captions(timeline["captions"], ass_path)
    cmd = build_ffmpeg_command(timeline, ass_path, out_path)

    job["status"] = "running"
    job["command"] = " ".join(cmd[:12]) + " …"
    s.put("render_jobs", job)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1200)
    except subprocess.TimeoutExpired:
        job["status"] = "failed"
        job["log"].append("render timed out after 1200s")
        return s.put("render_jobs", job)

    if proc.returncode != 0:
        job["status"] = "failed"
        job["log"].append(proc.stderr[-4000:])
        settings.log("render", f"FAILED {video_id}: see render job {job['id']}")
        return s.put("render_jobs", job)

    job["status"] = "done"
    job["output"] = str(out_path)
    job["placeholders_used"] = sum(1 for seg in timeline["segments"] if seg["placeholder"])
    s.put("render_jobs", job)

    video["file_path"] = str(out_path)
    video["duration_seconds"] = timeline["duration"] + timeline["end_card_seconds"]
    video["status"] = "needs_review"
    s.put("videos", video)
    settings.log("render", f"rendered {video_id} → {out_path.name} "
                           f"({job['placeholders_used']} placeholder segments)")
    return job


def timeline_json(video_id: str) -> str:
    """Debug/handoff helper: the timeline a renderer would consume."""
    s = store()
    video = s.get("videos", video_id) or {}
    script = s.get("scripts", video.get("script_id", "")) or {}
    matches = [{"shot_seq": sh.get("seq"), "asset_id": aid}
               for sh, aid in zip(script.get("shot_list", []), video.get("asset_ids", []))]
    return json.dumps(build_timeline(video, script, matches, video.get("vo_path")), indent=2)
