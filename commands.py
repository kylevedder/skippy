import subprocess
from typing import Tuple
from pathlib import Path


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
        process = subprocess.Popen(
            self.command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = process.communicate()
        return self.return_code_matcher(process.returncode), stdout, stderr

    def __repr__(self) -> str:
        return f"Shell(command={self.command})"


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
        self.filename = filename

    def run(self) -> Tuple[bool, str, str]:
        return Path(self.filename).is_file(), "", ""

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
