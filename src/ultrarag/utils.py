import ctypes
import logging
import os
import signal
import subprocess
from typing import Dict, List, Optional, Tuple
import re as _re
import html as _html
import json as _json
import codecs as _codecs

IS_POSIX = os.name == "posix"
IS_WINDOWS = os.name == "nt"

if IS_POSIX:
    libc = ctypes.CDLL(None)
else:
    libc = None

_windows_job_handle = None

if IS_WINDOWS:
    import ctypes.wintypes as wintypes

    JobObjectExtendedLimitInformation = 9
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    def _windows_ensure_job_object():
        global _windows_job_handle
        if _windows_job_handle:
            return _windows_job_handle

        hJob = kernel32.CreateJobObjectW(None, None)
        if not hJob:
            raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")

        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

        res = kernel32.SetInformationJobObject(
            hJob,
            JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
        if not res:
            err = ctypes.get_last_error()
            kernel32.CloseHandle(hJob)
            raise OSError(err, "SetInformationJobObject failed")

        _windows_job_handle = hJob
        return _windows_job_handle

    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        wintypes.INT,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL


def set_pdeathsig():
    if IS_POSIX and libc is not None:
        libc.prctl(1, signal.SIGTERM)


def popen_follow_parent(command: List[str], env: Optional[Dict[str, str]] = None):
    if IS_POSIX:
        return subprocess.Popen(command, env=env, preexec_fn=set_pdeathsig)
    elif IS_WINDOWS:
        hJob = _windows_ensure_job_object()
        proc = subprocess.Popen(command, env=env)
        import ctypes.wintypes as wintypes

        hProcess = wintypes.HANDLE(proc._handle)
        ok = kernel32.AssignProcessToJobObject(hJob, hProcess)
        if not ok:
            logging.warning(
                "AssignProcessToJobObject failed; child may outlive parent."
            )
        return proc
    else:
        return subprocess.Popen(command, env=env)


def normalize_readme_text(text: Optional[str]) -> Tuple[str, str]:
    """Normalize README content to readable plain text.

    Steps:
    - Try JSON decoding up to 2 passes to unwrap escaped payloads.
    - If a dict is found, prefer fields like ReadMeContent/readme/content/text.
    - Strip HTML tags (preserve basic line breaks) and HTML-unescape entities.

    Returns (clean_text, clean_state) where clean_state in
    {"html_stripped", "json_decoded", "raw"}.
    """
    if text is None:
        return "", "raw"

    s = str(text)
    clean_state = "raw"
    decoded = False

    # Attempt to decode JSON-escaped content up to two times
    for _ in range(2):
        s_stripped = s.strip()
        if not s_stripped:
            break
        try:
            obj = _json.loads(s_stripped)
        except Exception:
            # Not a top-level JSON payload; keep going to unicode-unescape
            pass
        else:
            decoded = True
            if isinstance(obj, str):
                s = obj
                continue
            if isinstance(obj, dict):
                for key in ("ReadMeContent", "readme", "ReadmeContent", "content", "text"):
                    val = obj.get(key)
                    if isinstance(val, str) and val.strip():
                        s = val
                        break
                else:
                    # Fallback: stringify to avoid returning an opaque dict
                    s = _json.dumps(obj, ensure_ascii=False)
                break
            # Non-dict/non-str payload: stop
            break
    # If we did not JSON-decode, try to extract embedded ReadMeContent value
    # from a larger JSON-like blob without full JSON integrity.
    if not decoded and "ReadMeContent" in s:
        m = _re.search(r"ReadMeContent\"\s*:\s*\"", s)
        if m:
            start = m.end()
            buf = []
            esc = False
            for ch in s[start:]:
                if esc:
                    buf.append(ch)
                    esc = False
                    continue
                if ch == "\\":
                    esc = True
                    continue
                if ch == '"':
                    break
                buf.append(ch)
            if buf:
                s = "".join(buf)
                decoded = True

    # If we did not JSON-decode, try to interpret unicode escape sequences
    # like "\u003c" that commonly appear in scraped payloads
    if not decoded and ("\\u" in s or _re.search(r"\\x[0-9a-fA-F]{2}", s)):
        for _ in range(2):
            try:
                s_new = _codecs.decode(s, "unicode_escape")
            except Exception:
                break
            if s_new == s:
                break
            s = s_new
            decoded = True

    if decoded:
        clean_state = "json_decoded"

    # Preserve basic line breaks for <br> then drop tags
    pre = s
    s = _re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = _re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", s)
    s_no_tags = _re.sub(r"(?s)<[^>]+>", "", s)
    if s_no_tags != pre:
        clean_state = "html_stripped"

    # Unescape HTML entities and normalize whitespace
    s_out = _html.unescape(s_no_tags)
    s_out = _re.sub(r"[ \t]+", " ", s_out)
    s_out = _re.sub(r"\n{3,}", "\n\n", s_out)
    s_out = s_out.strip()
    return s_out, clean_state
