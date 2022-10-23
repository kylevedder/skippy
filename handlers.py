from pathlib import Path
from twilio.rest import Client
from typing import Optional


class Handler:

    def handle(self, stdout: str, stderr: str) -> None:
        raise NotImplementedError


class PrintHandler(Handler):

    def __init__(self) -> None:
        pass

    def handle(self, stdout: str, stderr: str) -> None:
        print("STDOUT:")
        print(stdout)
        print("STDERR:")
        print(stderr)

    def __repr__(self) -> str:
        return f"PrintHandler()"


class TwilioHandler(Handler):

    def __init__(self, twilio_creds_file: Path, message: str) -> None:
        self.twilio_creds_file = Path(twilio_creds_file)
        assert self.twilio_creds_file.is_file(
        ), f"Twilio creds file {self.twilio_creds_file} does not exist"
        self.message = message

        # Read in the twilio creds file
        with open(self.twilio_creds_file, "r") as f:
            lines = f.readlines()
            self.account_sid = lines[0].strip()
            self.auth_token = lines[1].strip()
            self.from_number = lines[2].strip()
            self.to_number = lines[3].strip()

        # Create the twilio client
        self.client = Client(self.account_sid, self.auth_token)

    def handle(self, stdout: str, stderr: str) -> None:
        message_res = self.client.messages.create(
            to=self.to_number,
            from_=self.from_number,
            body=f"Pipeline {self.message}! See logs for details.",
        )
        print("Twilio message sent", message_res.sid, "with body",
              self.message)
