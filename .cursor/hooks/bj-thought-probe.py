#!/usr/bin/env python3
"""Observation-only Cursor hook used to find out whether a cloud agent's
thinking reaches hook scripts (afterAgentThought), and for which conversations.

It never blocks anything: it reads the event, records metadata, prints {} and
exits 0 whatever happens. It makes no network calls.

What it records
  /tmp/bj-thought-probe/events.jsonl   one line per event: time, event name,
                                       conversation id, model, text length,
                                       thinking duration, payload key names
  /tmp/bj-thought-probe/summary.json   per-conversation counters, plus the first
                                       80 characters of the first 2 thinking
                                       blocks of each conversation
  <store>/bj-thought-probe/<run>.json  a copy of summary.json in the user's
                                       persistent Cursor store, so the result
                                       can be read from another cloud machine

Remove the two files under .cursor/ to switch it off.
"""
import fcntl
import json
import os
import shutil
import socket
import subprocess
import sys
import time

PROBE = "bj-thought-probe v1"
LOCAL_DIR = os.environ.get("BJ_PROBE_LOCAL_DIR", "/tmp/bj-thought-probe")
STORE_ROOT = os.environ.get("BJ_PROBE_STORE_ROOT", "/cursor/stores/user")
STORE_SELF = os.environ.get("BJ_PROBE_STORE_SELF", "/cursor/stores/self")
SYNC_EVERY_S = 30
SYNC_NEW_AFTER_S = 3
SYNC_TIMEOUT_S = 4
HEAD_CHARS = 80
MAX_SAMPLES = 2


def run_id():
    try:
        name = os.path.basename(os.path.realpath(STORE_SELF))
        if name.startswith("bc-"):
            return name
    except Exception:
        pass
    return "host-" + socket.gethostname()


def load_summary(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("conversations"), dict):
            return data
    except Exception:
        pass
    return {"probe": PROBE, "run": run_id(), "host": socket.gethostname(),
            "first_event_at": None, "last_event_at": None,
            "last_sync_at": 0, "unsynced_new": False, "conversations": {}}


def sync_to_store(snapshot_path):
    target_dir = os.path.join(STORE_ROOT, "bj-thought-probe")
    target = os.path.join(target_dir, run_id() + ".json")
    script = "mkdir -p \"$1\" && cp \"$2\" \"$3\""
    subprocess.run(["sh", "-c", script, "sh", target_dir, snapshot_path, target],
                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL, timeout=SYNC_TIMEOUT_S, check=False)


def record(ev):
    now = time.time()
    name = str(ev.get("hook_event_name") or "unknown")
    conv = str(ev.get("conversation_id") or "")
    text = ev.get("text") if isinstance(ev.get("text"), str) else ""
    duration = ev.get("duration_ms")
    if not isinstance(duration, (int, float)):
        duration = None

    os.makedirs(LOCAL_DIR, exist_ok=True)
    line = {"t": round(now, 3), "event": name, "conversation_id": conv,
            "generation_id": ev.get("generation_id"), "model": ev.get("model"),
            "text_len": len(text), "duration_ms": duration,
            "keys": sorted(str(k) for k in ev.keys())}
    with open(os.path.join(LOCAL_DIR, "events.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps(line, ensure_ascii=False) + "\n")

    summary_path = os.path.join(LOCAL_DIR, "summary.json")
    snapshot_path = None
    with open(os.path.join(LOCAL_DIR, "lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        summary = load_summary(summary_path)
        convs = summary["conversations"]
        first_ever = not convs
        if conv not in convs:
            summary["unsynced_new"] = True
            convs[conv] = {"first_at": round(now, 3), "last_at": None,
                           "model": None, "events": {}, "thought_chars": 0,
                           "thought_ms": 0, "samples": []}
        c = convs[conv]
        if name not in c["events"]:
            summary["unsynced_new"] = True
        c["last_at"] = round(now, 3)
        c["model"] = ev.get("model") or c.get("model")
        c["events"][name] = int(c["events"].get(name, 0)) + 1
        if name == "afterAgentThought":
            c["thought_chars"] += len(text)
            c["thought_ms"] += int(duration or 0)
            if len(c["samples"]) < MAX_SAMPLES:
                c["samples"].append({"at": round(now, 3), "len": len(text),
                                     "duration_ms": duration,
                                     "head": " ".join(text[:HEAD_CHARS].split())})
        summary["first_event_at"] = summary.get("first_event_at") or round(now, 3)
        summary["last_event_at"] = round(now, 3)
        summary["cursor_version"] = ev.get("cursor_version") or summary.get("cursor_version")
        summary["workspace_roots"] = ev.get("workspace_roots") or summary.get("workspace_roots")
        summary["payload_keys"] = sorted(set(summary.get("payload_keys") or []) | set(line["keys"]))
        summary["env_names"] = sorted(k for k in os.environ if k.startswith(("CURSOR_", "CLAUDE_")))
        since = now - float(summary.get("last_sync_at") or 0)
        due = (first_ever or since >= SYNC_EVERY_S
               or (summary.get("unsynced_new") and since >= SYNC_NEW_AFTER_S))
        if due:
            summary["last_sync_at"] = round(now, 3)
            summary["unsynced_new"] = False
        tmp = summary_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=1)
        os.replace(tmp, summary_path)
        if due:
            snapshot_path = os.path.join(LOCAL_DIR, "sync-%d.json" % os.getpid())
            shutil.copyfile(summary_path, snapshot_path)
    if snapshot_path:
        try:
            sync_to_store(snapshot_path)
        finally:
            try:
                os.remove(snapshot_path)
            except OSError:
                pass


def main():
    try:
        raw = sys.stdin.read()
        ev = json.loads(raw) if raw.strip() else {}
        if isinstance(ev, dict) and ev:
            record(ev)
    except Exception as exc:
        try:
            os.makedirs(LOCAL_DIR, exist_ok=True)
            with open(os.path.join(LOCAL_DIR, "errors.log"), "a", encoding="utf-8") as f:
                f.write("%s %s: %s\n" % (time.strftime("%H:%M:%S"), type(exc).__name__, exc))
        except Exception:
            pass
    sys.stdout.write("{}\n")


if __name__ == "__main__":
    try:
        main()
        sys.stdout.flush()
    except BaseException:
        pass
    os._exit(0)
