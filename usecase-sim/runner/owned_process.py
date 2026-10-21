"""Owned child trees for host, Docker and nested simulator subprocesses.

Docker ownership is an opaque token plus a recorded process group, never a
script-name match. Windows uses a kill-on-close Job Object for descendants.
An uncertain remote teardown raises rather than allowing resource admission.
"""
from __future__ import annotations

import ctypes
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

OWNER_ENV = "DATACOOLIE_SIM_OWNER_DIR"
MODE_ENV = "DATACOOLIE_SIM_EXECUTION_MODE"
ROOT = Path(__file__).resolve().parents[2]
REMOTE_SCRIPT = "/datacoolie/usecase-sim/runner/owned_process.py"
CANCEL = threading.Event()


class TeardownError(RuntimeError):
    """Resources must remain withheld after unconfirmed child teardown."""


def docker_command(command):
    """Return structured container/argv for the supported exec options."""
    if command[:2] != ["docker", "exec"]:
        return None
    index = 2
    while index < len(command) and command[index].startswith("-"):
        option = command[index]
        if option in ("-e", "--env", "-w", "--workdir", "-u", "--user"):
            index += 2
        else:
            raise ValueError(f"Unsupported supervised Docker exec option: {option}")
    if index + 1 >= len(command):
        raise ValueError("Docker exec needs a container and command")
    return index, command[index], command[index + 1:]


class WindowsJob:
    def __init__(self, process):
        from ctypes import wintypes as w
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateJobObjectW.restype = w.HANDLE
        kernel.CreateJobObjectW.argtypes = [w.LPVOID, w.LPCWSTR]
        kernel.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, w.LPVOID, w.DWORD]
        kernel.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
        kernel.TerminateJobObject.argtypes = [w.HANDLE, w.UINT]
        kernel.CloseHandle.argtypes = [w.HANDLE]
        kernel.QueryInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, w.LPVOID, w.DWORD, w.LPVOID]
        # JOBOBJECT_EXTENDED_LIMIT_INFORMATION, with pointer-size alignment.
        class Basic(ctypes.Structure):
            _fields_ = [("user", ctypes.c_int64), ("job_user", ctypes.c_int64),
                        ("flags", w.DWORD), ("min", ctypes.c_size_t),
                        ("max", ctypes.c_size_t), ("active", w.DWORD),
                        ("affinity", ctypes.c_size_t), ("priority", w.DWORD),
                        ("scheduling", w.DWORD)]
        class Extended(ctypes.Structure):
            _fields_ = [("basic", Basic), ("io", ctypes.c_uint64 * 6),
                        ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                        ("peak_process", ctypes.c_size_t), ("peak_job", ctypes.c_size_t)]
        self.kernel, self.handle = kernel, kernel.CreateJobObjectW(None, None)
        limits = Extended()
        limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.handle or not kernel.SetInformationJobObject(
                self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)) or not kernel.AssignProcessToJobObject(
                self.handle, w.HANDLE(int(process._handle))):
            process.kill()
            self.close()
            raise TeardownError(f"Cannot establish Windows child ownership: {ctypes.get_last_error()}")
        self.resume(process.pid)

    def resume(self, pid):
        """The only child thread is suspended until Job ownership is attached."""
        from ctypes import wintypes as w
        class ThreadEntry(ctypes.Structure):
            _fields_ = [("size", w.DWORD), ("usage", w.DWORD), ("id", w.DWORD),
                        ("owner", w.DWORD), ("priority", w.LONG), ("delta", w.LONG), ("flags", w.DWORD)]
        kernel = self.kernel
        kernel.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
        kernel.CreateToolhelp32Snapshot.restype = w.HANDLE
        kernel.Thread32First.argtypes = [w.HANDLE, ctypes.POINTER(ThreadEntry)]
        kernel.Thread32Next.argtypes = [w.HANDLE, ctypes.POINTER(ThreadEntry)]
        kernel.OpenThread.argtypes = [w.DWORD, w.BOOL, w.DWORD]
        kernel.OpenThread.restype = w.HANDLE
        kernel.ResumeThread.argtypes = [w.HANDLE]
        kernel.ResumeThread.restype = w.DWORD
        snapshot = kernel.CreateToolhelp32Snapshot(4, 0)
        if snapshot == ctypes.c_void_p(-1).value:
            self.close()
            raise TeardownError("Cannot find suspended child thread")
        entry = ThreadEntry()
        entry.size = ctypes.sizeof(entry)
        found = False
        try:
            available = kernel.Thread32First(snapshot, ctypes.byref(entry))
            while available:
                if entry.owner == pid:
                    handle = kernel.OpenThread(2, False, entry.id)
                    if handle:
                        try:
                            found = kernel.ResumeThread(handle) == 1
                        finally:
                            kernel.CloseHandle(handle)
                    break
                available = kernel.Thread32Next(snapshot, ctypes.byref(entry))
        finally:
            kernel.CloseHandle(snapshot)
        if not found:
            self.close()
            raise TeardownError("Cannot resume owned child thread")

    def kill(self):
        if not self.kernel.TerminateJobObject(self.handle, 124):
            raise TeardownError("Windows Job Object termination failed")
        for _ in range(100):
            if not self.active():
                return
            time.sleep(.05)
        raise TeardownError("Windows child descendants did not terminate")

    def active(self):
        class Accounting(ctypes.Structure):
            _fields_ = [("times", ctypes.c_int64 * 4), ("faults", ctypes.c_uint32),
                        ("total", ctypes.c_uint32), ("active", ctypes.c_uint32),
                        ("terminated", ctypes.c_uint32)]
        info = Accounting()
        if not self.kernel.QueryInformationJobObject(self.handle, 1, ctypes.byref(info), ctypes.sizeof(info), None):
            raise TeardownError("Windows descendant accounting failed")
        return info.active

    def close(self):
        if self.handle:
            if self.active():
                self.kill()
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def _control(container, token, action, group=None):
    if container is None:
        if action != "check":
            _signal_owned(group, token, getattr(signal, "SIG" + action))
        empty = not _members(group)
        return {"finished": empty, "teardown": empty}
    arguments = ["control", token, action] + ([str(group)] if group else [])
    result = subprocess.run(["docker", "exec", container, "python3", REMOTE_SCRIPT,
                             *arguments], capture_output=True, text=True, timeout=15)
    if result.returncode:
        raise TeardownError(f"Container child control failed: {result.stderr.strip()}")
    return json.loads(result.stdout)


def _remote_cleanup(directory):
    for registration in directory.glob("*.json"):
        owner = json.loads(registration.read_text())
        state = _control(owner["container"], owner["token"], "check", owner.get("group"))
        if not state.get("finished"):
            _control(owner["container"], owner["token"], "KILL", owner.get("group"))
            for _ in range(50):
                state = _control(owner["container"], owner["token"], "check", owner.get("group"))
                if state.get("finished"):
                    break
                time.sleep(.1)
        if not state.get("teardown"):
            raise TeardownError(f"Unconfirmed container teardown: {owner}")


def run_owned(command, *, cwd, timeout, on_line=None, grace=120):
    """Capture/stream a child; nested callers inherit the outer owner directory."""
    parent = os.environ.get(OWNER_ENV)
    directory = Path(parent) if parent else ROOT / "usecase-sim/.runtime/logs/scenarios/processes" / uuid.uuid4().hex
    directory.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, **{OWNER_ENV: str(directory), "PYTHONUNBUFFERED": "1"})
    launch = list(command)
    remote = docker_command(launch)
    token = uuid.uuid4().hex
    env["DATACOOLIE_SIM_PROCESS_TOKEN"] = token
    if env.get(MODE_ENV) == "optimized":
        for key in ("POLARS_MAX_THREADS", "RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS"):
            env.setdefault(key, "4")
    if remote:
        index, container, arguments = remote
        (directory / f"{token}.json").write_text(json.dumps({"container": container, "token": token}))
        owner_path = "/datacoolie/" + directory.relative_to(ROOT).as_posix()
        launch = launch[:index] + ["-e", f"{OWNER_ENV}={owner_path}", "-e",
                    f"DATACOOLIE_SIM_CONTAINER={container}", "-e",
                    f"{MODE_ENV}={env.get(MODE_ENV, 'sequential')}", container,
                    "python3", REMOTE_SCRIPT, "remote", token, *arguments]
    options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | 4} if os.name == "nt" else {"start_new_session": True}
    process = subprocess.Popen(launch, cwd=cwd, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", **options)
    job = WindowsJob(process) if os.name == "nt" else None
    if not remote and os.name != "nt":
        (directory / f"{token}.json").write_text(json.dumps({"container": env.get("DATACOOLIE_SIM_CONTAINER"),
                                                          "group": process.pid, "token": token}))
    expired, cancelled, errors = threading.Event(), threading.Event(), []

    def cancel(hard=False):
        if remote:
            _control(container, token, "KILL" if hard else "TERM")
        else:
            for registration in directory.glob("*.json"):
                owner = json.loads(registration.read_text())
                _control(owner["container"], owner["token"], "KILL" if hard else "TERM", owner.get("group"))
            if job and hard:
                job.kill()
            elif os.name == "nt":
                process.send_signal(signal.CTRL_BREAK_EVENT)
            # POSIX children are registered above and were already signalled
            # through _control. A second signal races their process exit.

    def watchdog():
        try:
            deadline = time.monotonic() + timeout
            while process.poll() is None and time.monotonic() < deadline and not CANCEL.wait(.1):
                pass
            if process.poll() is not None:
                # A descendant may still own stdout after the immediate child
                # exits. Reap it here so EOF cannot bypass the timeout boundary.
                if job and job.active():
                    job.kill()
                elif not remote and os.name != "nt":
                    _signal_owned(process.pid, token, signal.SIGKILL)
                return
            (cancelled if CANCEL.is_set() else expired).set()
            cancel()
            try:
                process.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                cancel(True)
                process.wait(timeout=30)
        except BaseException as exc:
            errors.append(exc)
            # The caller will fail closed, but stdout must be allowed to end.
            if job:
                job.kill()
            else:
                process.kill()

    watcher = threading.Thread(target=watchdog, daemon=True)
    watcher.start()
    output = []
    try:
        for line in process.stdout:
            output.append(line)
            if on_line:
                on_line(line)
        process.wait()
        watcher.join()
    except BaseException:
        cancel()
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            cancel(True)
            process.wait(timeout=30)
        raise
    finally:
        if job:
            job.close()
        elif not remote:
            try:
                _signal_owned(process.pid, token, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if not parent:
            _remote_cleanup(directory)
    if errors:
        raise TeardownError(str(errors[0])) from errors[0]
    result = subprocess.CompletedProcess(command, process.returncode, "".join(output), "")
    result.execution = {"command": launch, "host_pid": process.pid, "token": token,
                        "container": remote[1] if remote else None,
                        "supervisor": "docker_group" if remote else "windows_job" if job else "posix_group"}
    if expired.is_set():
        failure = subprocess.TimeoutExpired(command, timeout, output=result.stdout)
        failure.execution = result.execution
        raise failure
    if cancelled.is_set():
        raise InterruptedError("Simulator suite cancelled; owned child teardown confirmed")
    return result


def _state_path(token):
    if len(token) != 32 or any(c not in "0123456789abcdef" for c in token):
        raise ValueError("Invalid process ownership token")
    return Path("/tmp/datacoolie-usecase-sim/processes") / f"{token}.json"


def _members(group):
    members = []
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            fields = path.read_text().rsplit(") ", 1)[1].split()
            if int(fields[2]) == group and fields[0] != "Z":
                members.append(int(path.parent.name))
        except (OSError, ValueError, IndexError):
            pass
    return members


def _remote(token, command):
    path = _state_path(token)
    path.parent.mkdir(parents=True, exist_ok=True)
    child = subprocess.Popen(command, start_new_session=True,
                             env=dict(os.environ, DATACOOLIE_SIM_PROCESS_TOKEN=token))
    state = {"group": child.pid, "finished": False, "teardown": False}
    path.write_text(json.dumps(state))
    try:
        code = child.wait()
    finally:
        if _members(child.pid):
            _signal_owned(child.pid, token, signal.SIGTERM)
            for _ in range(20):
                if not _members(child.pid):
                    break
                time.sleep(.1)
            if _members(child.pid):
                _signal_owned(child.pid, token, signal.SIGKILL)
        child.wait()
        for _ in range(20):
            if not _members(child.pid):
                break
            time.sleep(.1)
        state.update(finished=True, teardown=not _members(child.pid))
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state))
        temporary.replace(path)
    return code if state["teardown"] else 125


def _signal_owned(group, token, sig):
    members = _members(group)
    verified = False
    for pid in members:
        try:
            environment = (Path("/proc") / str(pid) / "environ").read_bytes().split(b"\0")
        except (FileNotFoundError, ProcessLookupError):
            # /proc can report ENOENT or ESRCH while a member exits.
            continue
        if f"DATACOOLIE_SIM_PROCESS_TOKEN={token}".encode() not in environment:
            # A live member can exit between /proc/stat and /proc/environ;
            # zombies have an empty environment. Recheck liveness before
            # treating this as a foreign process. Alive mismatches still fail.
            if pid not in _members(group):
                continue
            raise TeardownError("Process group identity changed; signal withheld")
        verified = True
    if verified:
        try:
            os.killpg(group, sig)
        except ProcessLookupError:
            pass


if __name__ == "__main__":
    kind, token, *arguments = sys.argv[1:]
    if kind == "remote":
        raise SystemExit(_remote(token, arguments))
    if len(arguments) == 2:
        group = int(arguments[1])
        if arguments[0] != "check":
            _signal_owned(group, token, getattr(signal, "SIG" + arguments[0]))
        empty = not _members(group)
        print(json.dumps({"finished": empty, "teardown": empty}))
        raise SystemExit(0)
    path = _state_path(token)
    state = json.loads(path.read_text()) if path.exists() else {"finished": False, "teardown": False}
    if state.get("group") and not _members(state["group"]):
        # The wrapper may have stopped before updating its final record.
        # An empty recorded group is direct evidence of descendant teardown.
        state.update(finished=True, teardown=True)
    if arguments[0] != "check" and not state["finished"] and state.get("group"):
        try:
            _signal_owned(state["group"], token, getattr(signal, "SIG" + arguments[0]))
        except ProcessLookupError:
            pass
    print(json.dumps(state))
