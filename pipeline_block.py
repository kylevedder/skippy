from tkinter import N
from commands import Runner
from handlers import Handler
from typing import List, Optional, Tuple


class PipelineBlock:

    def __init__(self,
                 command: Runner,
                 preconditions: List[Runner] = [],
                 postconditions: List[Runner] = [],
                 failure_handler: Optional[Handler] = None,
                 success_handler: Optional[Handler] = None) -> None:
        self.command = command
        self.preconditions = preconditions
        self.postconditions = postconditions
        self.failure_handler = failure_handler
        self.success_handler = success_handler

    def _run(self, verbose: bool = False) -> Tuple[bool, str, str]:
        for precondition in self.preconditions:
            if verbose:
                print(f"Running precondition {precondition}")
            success, stdout, stderr = precondition.run()
            if not success:
                return False, stdout, stderr

        if verbose:
            print(f"Running command: {self.command}")
        success, stdout, stderr = self.command.run()
        if not success:
            return False, stdout, stderr

        for postcondition in self.postconditions:
            if verbose:
                print(f"Running postcondition {postcondition}")
            success, stdout, stderr = postcondition.run()
            if not success:
                if verbose:
                    print(f"Postcondition {postcondition} failed with stdout: {stdout} and stderr: {stderr}")
                return False, stdout, stderr

        return True, stdout, stderr

    def run(self) -> bool:
        success, stdout, stderr = self._run()
        if not success and self.failure_handler:
            self.failure_handler.handle(stdout, stderr)
        elif success and self.success_handler:
            self.success_handler.handle(stdout, stderr)
        return success
