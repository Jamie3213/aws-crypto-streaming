class TiingoClientError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(
            f"Failed to get next API message with error {self.code}: {self.message!r}."
        )


class TiingoMessageError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(
            f"Failed to get message with error {self.code}: {self.message!r}."
        )


class TiingoSubscriptionError(Exception):
    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(
            f"API subscription failed with error {self.code}: {self.message!r}."
        )


class TiingoBatchSizeError(Exception):
    def __init__(self, size: int) -> None:
        self.size = size
        super().__init__(f"Batch size must be at least 1, not {size}.")
