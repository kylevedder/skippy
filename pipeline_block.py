from commands import Runner
from typing import List, Optional


class PipelineBlock:

    def __init__(self,
                 command: Runner,
                 preconditions: List[Runner] = [],
                 postconditions: List[Runner] = [],
                 failure_handler: Optional[Runner] = None) -> None:
        self.command = command
        self.preconditions = preconditions
        self.postconditions = postconditions
        self.failure_handler = failure_handler

    def _run(self) -> bool:
        for precondition in self.preconditions:
            success, stdout, stderr = precondition.run()
            if not success:
                return False

        success, stdout, stderr = self.command.run()
        if not success:
            return False

        for postcondition in self.postconditions:
            success, stdout, stderr = postcondition.run()
            if not success:
                return False

        return True

    def run(self) -> bool:
        res = self._run()
        if not res and self.failure_handler:
            self.failure_handler.run()
        return res
