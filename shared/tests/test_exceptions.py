import pytest
from shared.exceptions import (
    CircuitBreakerOpenError,
    DatabaseDeadlockError,
    IdempotencyConflictError,
    InsufficientFundsError,
    InventoryExhaustedError,
    PoisonMessageError,
    SagaAbortError,
    SagaError,
    TokenValidationError,
)


class TestSagaError:
    def test_base_exception(self):
        err = SagaError("something failed")
        assert str(err) == "something failed"
        assert err.transaction_id is None

    def test_with_transaction_id(self):
        err = SagaError("failed", transaction_id="txn-123")
        assert err.transaction_id == "txn-123"

    def test_is_exception(self):
        assert issubclass(SagaError, Exception)


class TestSagaAbortError:
    def test_subclass_of_saga_error(self):
        assert issubclass(SagaAbortError, SagaError)


class TestInsufficientFundsError:
    def test_default_message(self):
        err = InsufficientFundsError()
        assert "Insufficient wallet balance" in str(err)
        assert err.transaction_id is None

    def test_custom_message(self):
        err = InsufficientFundsError("custom", transaction_id="txn-abc")
        assert str(err) == "custom"
        assert err.transaction_id == "txn-abc"

    def test_subclass_chain(self):
        assert issubclass(InsufficientFundsError, SagaAbortError)
        assert issubclass(InsufficientFundsError, SagaError)


class TestInventoryExhaustedError:
    def test_default_message(self):
        err = InventoryExhaustedError()
        assert "Inventory exhausted" in str(err)

    def test_subclass_chain(self):
        assert issubclass(InventoryExhaustedError, SagaAbortError)
        assert issubclass(InventoryExhaustedError, SagaError)


class TestIdempotencyConflictError:
    def test_default_message(self):
        err = IdempotencyConflictError()
        assert "Idempotency key conflict" in str(err)

    def test_subclass_of_saga_error(self):
        assert issubclass(IdempotencyConflictError, SagaError)
        assert not issubclass(IdempotencyConflictError, SagaAbortError)


class TestPoisonMessageError:
    def test_default_message(self):
        err = PoisonMessageError()
        assert "Unprocessable" in str(err)

    def test_subclass_of_saga_error(self):
        assert issubclass(PoisonMessageError, SagaError)


class TestCircuitBreakerOpenError:
    def test_default_message(self):
        err = CircuitBreakerOpenError()
        assert "Circuit breaker" in str(err)

    def test_subclass_of_saga_error(self):
        assert issubclass(CircuitBreakerOpenError, SagaError)


class TestTokenValidationError:
    def test_default_message(self):
        err = TokenValidationError()
        assert "Token validation" in str(err)

    def test_subclass_of_saga_error(self):
        assert issubclass(TokenValidationError, SagaError)


class TestDatabaseDeadlockError:
    def test_default_message(self):
        err = DatabaseDeadlockError()
        assert "deadlock" in str(err)

    def test_subclass_of_saga_error(self):
        assert issubclass(DatabaseDeadlockError, SagaError)

    def test_catchable_as_saga_error(self):
        with pytest.raises(SagaError):
            raise DatabaseDeadlockError(transaction_id="txn-deadlock")
