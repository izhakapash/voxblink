#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VoxBlink2 Downloader – Parallel Edition (2025)
----------------------------------------------
תומך ב:
- חיתוך ישיר (DIRECT_CUT)
- מחיקת קבצי ביניים (KEEP_RAW=False)
- מספר workers מקביליים לקבצי וידאו (WORKERS)
"""

from __future__ import annotations
from pathlib import Path
import sys, subprocess, tarfile, json, re, csv, shutil
from typing import Optional, Tuple, List
from tqdm import tqdm
from datetime import datetime
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ===================== CONFIG =====================
META_SRC   = Path(r"C:\Users\izhak\OneDrive\Desktop\voxblink_2\temp\meta\0\00")
SAVE_DIR   = Path(r"C:\vox_blink_2\output_2")
TMP_DIR    = Path(r"C:\vox_blink_2\temp")
SR         = 16000

USE_BROWSER_COOKIES = False
COOKIES_TXT: Optional[Path] = None
FORCE_IPV4  = False
CHUNK_SIZE  = None
AUDIO_FORMAT_SELECTOR = 'bestaudio'


WORKERS    = 10


DIRECT_CUT = True

KEEP_RAW   = False
# ==================================================

YT_ID_RE = re.compile(r'([A-Za-z0-9_-]{11})')
_BAR_LOCK = Lock()

def _fail(msg: str): print(msg); sys.exit(1)
def _mkdir(p: Path): p.mkdir(parents=True, exist_ok=True)

def _check_bins():
    try:
        subprocess.run(["ffmpeg","-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    except Exception:
        _fail("[ERR] ffmpeg not in PATH")
    exe = shutil.which("yt-dlp") or shutil.which("yt-dlp.exe")
    if not exe:
        _fail("[ERR] yt-dlp not found globally. Install via: pip install -U yt-dlp")

def _yt_dlp_cmd():
    exe = shutil.which("yt-dlp") or shutil.which("yt-dlp.exe")
    return [exe] if exe else [sys.executable, "-m", "yt_dlp"]

def _extract_video_id(name: str) -> Optional[str]:
    m = YT_ID_RE.search(name)
    return m.group(1) if m else None

def _meta_dir_from_src(meta_src: Path, tmp_dir: Path) -> Path:
    if meta_src.is_file() and meta_src.name.lower().endswith(".tar.gz"):
        meta_dir = tmp_dir / "meta"
        if meta_dir.exists() and any(meta_dir.iterdir()):
            return meta_dir
        print(f"[INF] Extracting {meta_src} -> {tmp_dir}")
        _mkdir(tmp_dir)
        with tarfile.open(meta_src, "r:gz") as tf:
            tf.extractall(tmp_dir)
        for p in tmp_dir.rglob("meta"):
            if p.is_dir(): return p
        _fail("[ERR] Could not find meta/ after extraction")
    if (meta_src / "meta").is_dir(): return meta_src / "meta"
    if meta_src.is_dir(): return meta_src
    _fail(f"[ERR] META_SRC invalid: {meta_src}")

def _parse_ts(fp: Path) -> Optional[Tuple[float,float]]:
    raw = fp.read_text(encoding="utf-8",errors="ignore").strip()
    m = re.search(r"Start\s+and\s+Duration\s*:\s*([0-9]*\.?[0-9]+)\s+([0-9]*\.?[0-9]+)",raw,re.I)
    if m: s=float(m[1]); dur=float(m[2]); return (s,s+dur)
    try:
        obj=json.loads(raw); s=obj.get("start") or obj.get("st"); e=obj.get("end") or obj.get("ed")
        if s is not None and e is not None: return (float(s),float(e))
    except: pass
    parts=re.findall(r"[0-9]*\.?[0-9]+",raw)
    if len(parts)>=2: return (float(parts[0]),float(parts[1]))
    return None

def _iter_jobs(meta_dir: Path) -> List[Tuple[str,Path,str]]:
    jobs=[]
    for spk in meta_dir.iterdir():
        if not spk.is_dir(): continue
        for vid_dir in spk.iterdir():
            if vid_dir.is_dir() and list(vid_dir.glob("*.txt")):
                vid=_extract_video_id(vid_dir.name)
                if vid: jobs.append((spk.name, vid_dir, vid))
    print(f"[INF] Prepared {len(jobs)} videos for processing")
    return jobs

# הורדה מלאה (אם DIRECT_CUT=False)
def _download_full_wav(video_id: str, raw_dir: Path, sr: int, cookies_txt: Optional[Path], logs_dir: Path):
    _mkdir(raw_dir); _mkdir(logs_dir)
    url=f"https://www.youtube.com/watch?v={video_id}"
    out_tmpl=str(raw_dir / "%(id)s.%(ext)s")
    vlog=logs_dir / f"{video_id}.yt-dlp.log"

    cmd=_yt_dlp_cmd()+[
        "--ignore-config","-v","-f",AUDIO_FORMAT_SELECTOR,
        "--extract-audio","--audio-format","wav",
        "--postprocessor-args",f"ffmpeg:-ar {sr} -ac 1",
        "--retries","20","--fragment-retries","20",
        "--sleep-requests","1","--sleep-interval","1","--max-sleep-interval","5",
        "--no-playlist","--geo-bypass","-o",out_tmpl,url,
    ]
    if FORCE_IPV4: cmd.insert(1,"--force-ipv4")
    if CHUNK_SIZE: cmd += ["--http-chunk-size", CHUNK_SIZE]
    if USE_BROWSER_COOKIES:
        cmd += ["--cookies-from-browser","chrome:profile=Default"]
    elif cookies_txt and cookies_txt.exists():
        cmd += ["--cookies", str(cookies_txt)]

    try:
        with vlog.open("w",encoding="utf-8") as lf:
            proc=subprocess.run(cmd,stdout=lf,stderr=lf)
            rc=proc.returncode
    except Exception as e:
        (logs_dir / f"{video_id}.python.err.txt").write_text(str(e))
        return None,"python-exception"

    wav=raw_dir / f"{video_id}.wav"
    if wav.exists() and rc==0: return wav,""
    return None,"failed"

def _cut_utts_from_raw(spk:str,vid_dir:Path,raw_wav:Path,save_dir:Path,sr:int,logs_dir:Path)->int:
    ok=0
    for utt in sorted(vid_dir.glob("*.txt")):
        ts=_parse_ts(utt)
        if not ts: continue
        s,e=ts
        if e<=s: continue
        dst=save_dir / spk / vid_dir.name / (utt.stem+".wav")
        dst.parent.mkdir(parents=True,exist_ok=True)
        cmd=["ffmpeg","-hide_banner","-loglevel","error","-ss",f"{s:.3f}","-t",f"{(e-s):.3f}","-i",str(raw_wav),
             "-ac","1","-ar",str(sr),str(dst)]
        try: subprocess.run(cmd,check=True); ok+=1
        except subprocess.CalledProcessError as e: (logs_dir/f"{vid_dir.name}.{utt.stem}.err.txt").write_text(str(e))
    return ok

def _get_remote_audio_url(video_id: str) -> Optional[str]:
    cmd = _yt_dlp_cmd() + ["-f", AUDIO_FORMAT_SELECTOR, "-g", f"https://www.youtube.com/watch?v={video_id}"]
    try: out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip().splitlines()
    except Exception: return None
    return out[-1] if out else None

def _cut_from_remote(url: str, s: float, e: float, dst: Path, sr: int) -> bool:
    dst.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["ffmpeg","-hide_banner","-loglevel","error","-ss",f"{s:.3f}","-to",f"{e:.3f}","-i",url,"-ac","1","-ar",str(sr),str(dst)]
    return subprocess.run(cmd).returncode == 0

def _write_summary(rows:List[dict],out_dir:Path):
    _mkdir(out_dir)
    csvp=out_dir/"_summary.csv"
    with csvp.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["speaker","video_id","clips_ok","status"])
        w.writeheader(); [w.writerow(r) for r in rows]
    print("[OK] Summary written:", csvp)

def _process_one_video(spk:str,vid_dir:Path,vid_id:str,save_dir:Path,sr:int,logs_root:Path,raw_root:Path,cookies_txt:Optional[Path],
                       bar_vid:"tqdm",bar_utt:"tqdm")->dict:
    vlogs = logs_root / vid_id; _mkdir(vlogs)
    if DIRECT_CUT:
        url = _get_remote_audio_url(vid_id)
        if not url:
            with _BAR_LOCK: bar_vid.update(1)
            return {"speaker":spk,"video_id":vid_id,"clips_ok":0,"status":"no-url"}
        n_ok=0
        for utt in sorted(vid_dir.glob("*.txt")):
            ts=_parse_ts(utt)
            if not ts: continue
            s,e=ts
            if e<=s: continue
            dst=save_dir/spk/vid_dir.name/(utt.stem+".wav")
            if _cut_from_remote(url,s,e,dst,sr): n_ok+=1
        with _BAR_LOCK:
            bar_vid.update(1); bar_utt.update(n_ok)
        return {"speaker":spk,"video_id":vid_id,"clips_ok":n_ok,"status":"ok" if n_ok>0 else "no-valid-ts"}

    # מצב לא-ישיר
    raw,fail=_download_full_wav(vid_id,raw_root,sr,cookies_txt,vlogs)
    if not raw:
        with _BAR_LOCK: bar_vid.update(1)
        return {"speaker":spk,"video_id":vid_id,"clips_ok":0,"status":f"download-failed:{fail}"}
    n_ok=_cut_utts_from_raw(spk,vid_dir,raw,save_dir,sr,vlogs)
    if not KEEP_RAW:
        try: raw.unlink()
        except: pass
    with _BAR_LOCK:
        bar_vid.update(1); bar_utt.update(n_ok)
    return {"speaker":spk,"video_id":vid_id,"clips_ok":n_ok,"status":"ok"}

def build_full_then_cut(meta_dir:Path,save_dir:Path,sr:int,cookies_txt:Optional[Path]):
    jobs=_iter_jobs(meta_dir)
    total_utts=sum(len(list(v.glob("*.txt"))) for _,v,_ in jobs)
    print(f"[INF] Total utterances: {total_utts}")
    shutil.rmtree(TMP_DIR/"raw_full",ignore_errors=True)
    raw_root=TMP_DIR/"raw_full"; logs_root=save_dir/"_logs"
    _mkdir(raw_root); _mkdir(save_dir); _mkdir(logs_root)
    bar_vid=tqdm(total=len(jobs),desc="Videos",unit="vid")
    bar_utt=tqdm(total=total_utts,desc="Utterances",unit="utt")
    rows=[]
    with ThreadPoolExecutor(max_workers=WORKERS) as exe:
        futures=[exe.submit(_process_one_video,spk,vid_dir,vid_id,save_dir,sr,logs_root,raw_root,cookies_txt,bar_vid,bar_utt)
                 for spk,vid_dir,vid_id in jobs]
        for fut in as_completed(futures): rows.append(fut.result())
    bar_vid.close(); bar_utt.close()
    print(f"[OK] Done: {sum(r['status']=='ok' for r in rows)} ok, {sum('download-failed' in r['status'] for r in rows)} failed")
    _write_summary(rows,save_dir)
    shutil.rmtree(TMP_DIR/"raw_full",ignore_errors=True)

def main():
    print(f"Running Python from: {sys.executable}")
    _check_bins()
    _mkdir(SAVE_DIR); _mkdir(TMP_DIR)
    meta=_meta_dir_from_src(META_SRC,TMP_DIR)
    build_full_then_cut(meta,SAVE_DIR,SR,COOKIES_TXT)
    print("\n[DONE] VoxBlink2 complete.")
    print(f" Clips: {SAVE_DIR}\n Logs:  {SAVE_DIR/'_logs'}")

if __name__=="__main__":
    main()
