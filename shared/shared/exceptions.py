class SagaError(Exception):
    def __init__(self, message: str, transaction_id: str | None = None):
        self.transaction_id = transaction_id
        super().__init__(message)


class SagaAbortError(SagaError):
    pass


class InsufficientFundsError(SagaAbortError):
    def __init__(
        self,
        message: str = "Insufficient wallet balance",
        transaction_id: str | None = None,
    ):
        super().__init__(message, transaction_id)


class InventoryExhaustedError(SagaAbortError):
    def __init__(
        self,
        message: str = "Inventory exhausted",
        transaction_id: str | None = None,
    ):
        super().__init__(message, transaction_id)


class IdempotencyConflictError(SagaError):
    def __init__(
        self,
        message: str = "Idempotency key conflict",
        transaction_id: str | None = None,
    ):
        super().__init__(message, transaction_id)


class PoisonMessageError(SagaError):
    def __init__(
        self,
        message: str = "Unprocessable message payload",
        transaction_id: str | None = None,
    ):
        super().__init__(message, transaction_id)


class CircuitBreakerOpenError(SagaError):
    def __init__(
        self,
        message: str = "Circuit breaker is open",
        transaction_id: str | None = None,
    ):
        super().__init__(message, transaction_id)


class TokenValidationError(SagaError):
    def __init__(
        self,
        message: str = "Token validation failed",
        transaction_id: str | None = None,
    ):
        super().__init__(message, transaction_id)


class DatabaseDeadlockError(SagaError):
    def __init__(
        self,
        message: str = "Database deadlock detected",
        transaction_id: str | None = None,
    ):
        super().__init__(message, transaction_id)
