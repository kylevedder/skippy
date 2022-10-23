import subprocess
from typing import Tuple, Optional
from pathlib import Path
import time


def _run_command(command: str, cwd: Path = Path()) -> Tuple[int, str, str]:
    print(f"running command: {command}")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )
    stdout, stderr = process.communicate()
    print("command finished")
    return process.returncode, stdout, stderr


class Runner:

    def run(self) -> Tuple[bool, str, str]:
        raise NotImplementedError


class LocalShell(Runner):

    def __init__(self, command: str, return_code_matcher=lambda x:
                 (x == 0)) -> None:
        self.command = command
        self.return_code_matcher = return_code_matcher

    def run(self) -> Tuple[bool, str, str]:
        """Run the command and return success, stdout, stderr"""
        ret_code, stdout, stderr = _run_command(self.command)
        return self.return_code_matcher(ret_code), stdout, stderr

    def __repr__(self) -> str:
        return f"Shell(command={self.command})"


class Force(Runner):

    def __init__(self, command: Runner) -> None:
        self.command = command

    def run(self) -> Tuple[bool, str, str]:
        _, stdout, stderr = self.command.run()
        return (True), stdout, stderr

    def __repr__(self) -> str:
        return f"Force(command={self.command})"


class Printer(Runner):

    def __init__(self, message: str) -> None:
        self.message = message

    def run(self) -> Tuple[bool, str, str]:
        print(self.message)
        return True, "", ""

    def __repr__(self) -> str:
        return f"Printer(message={self.message})"


class Not(Runner):

    def __init__(self, command: Runner) -> None:
        self.command = command

    def run(self) -> Tuple[bool, str, str]:
        res, stdout, stderr = self.command.run()
        return (not res), stdout, stderr

    def __repr__(self) -> str:
        return f"Not(command={self.command})"


class FileExists(Runner):

    def __init__(self, filename: str) -> None:
        self.filename = Path(filename)

    def run(self) -> Tuple[bool, str, str]:
        # There is something wrong with the exists() and is_file() methods.
        exists = Path(str(self.filename)) in list(
            self.filename.parent.glob('*'))
        return exists, "", ""

    def __repr__(self) -> str:
        return f"FileExists(filename={self.filename})"


class FilesExist(Runner):

    def __init__(self, pattern: str, directory: Path = Path()) -> None:
        self.directory = directory
        self.pattern = pattern

    def run(self) -> Tuple[bool, str, str]:
        if not self.directory.is_dir():
            return False, "", ""
        matched_files = [
            e for e in self.directory.glob(self.pattern) if e.is_file()
        ]
        return len(matched_files) > 0, "", ""

    def __repr__(self) -> str:
        return f"FilesExist(pattern={self.pattern}, directory={self.directory})"


class FolderExists(Runner):

    def __init__(self, foldername: str) -> None:
        self.foldername = Path(foldername)

    def run(self) -> Tuple[bool, str, str]:
        exists = Path(str(self.foldername)) in list(
            self.foldername.parent.glob('*'))
        return exists, "", ""

    def __repr__(self) -> str:
        return f"FolderExists(foldername={self.foldername})"


class FoldersExist(Runner):

    def __init__(self, pattern: str, directory: Path = Path()) -> None:
        self.directory = directory
        self.pattern = pattern

    def run(self) -> Tuple[bool, str, str]:
        if not self.directory.is_dir():
            return False, "", ""
        matched_folders = [
            e for e in self.directory.glob(self.pattern) if e.is_dir()
        ]
        return len(matched_folders) > 0, "", ""

    def __repr__(self) -> str:
        return f"FoldersExist(pattern={self.pattern}, directory={self.directory})"


class Rm(Runner):

    def __init__(self, filename: str) -> None:
        self.filename = filename

    def run(self) -> Tuple[bool, str, str]:
        if Path(self.filename).exists():
            Path(self.filename).unlink()
            return True, "", ""
        else:
            return False, "", ""

    def __repr__(self) -> str:
        return f"Rm(filename={self.filename})"


class Jlaunch3d(Runner):

    def __init__(self,
                 server_id: int,
                 command: str,
                 heartbeat_freq_secs: int = 5) -> None:
        self.server_id = server_id
        self.command = command
        self.heartbeat_freq_secs = heartbeat_freq_secs

    def _start_job(self) -> Optional[Tuple[str, str]]:
        ret_code, stdout, stderr = _run_command(
            f"cd /efs/code/partial_positives/; jlaunch3d {self.server_id} \"{self.command}\"",
            cwd=Path("/efs/code/partial_positives/"))
        if ret_code != 0:
            return None

        split_lines = stdout.strip().split("\n")

        ssh_target = split_lines[-2].strip()
        if not ssh_target.startswith("ssh target: "):
            return None
        ssh_target = ssh_target[len("ssh target: "):]

        screen_name = split_lines[-1].strip()
        if not screen_name.startswith("Screen name: "):
            return None
        screen_name = screen_name[len("Screen name: "):]
        return screen_name, ssh_target

    def _get_running_screens(self, t):
        cmd = f"ssh -o ConnectTimeout=1  {t} \"ls -1 /var/run/screen/S-ubuntu/\""
        print(cmd)
        ret_code, stdout, stderr = _run_command(cmd)
        print("Heartbeat result", ret_code, stdout, stderr)
        return ret_code == 0, stdout, t

    def _send_heartbeat(self, screen_name, ssh_target) -> Optional[bool]:
        conn_success, stdout, stderr = self._get_running_screens(ssh_target)
        if not conn_success:
            return None
        return screen_name in stdout

    def run(self) -> Tuple[bool, str, str]:
        """Run the command and return success, stdout, stderr"""
        res = self._start_job()
        if res is None:
            print("Failed to start job")
            return False, "", ""

        screen_name, ssh_target = res

        print("Sending heartbeat")
        heartbeat_result = self._send_heartbeat(screen_name, ssh_target)
        while heartbeat_result is not None:
            if heartbeat_result is False:
                print("Job exited")
                break
            time.sleep(self.heartbeat_freq_secs)
            print("Sending heartbeat")
            heartbeat_result = self._send_heartbeat(screen_name, ssh_target)

        # Heartbeat failed
        if heartbeat_result is None:
            print("Failed to connect to job")
            return False, "", ""

        return True, "", ""
