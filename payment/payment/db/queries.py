INSERT_IDEMPOTENCY = """
    INSERT INTO idempotency_store (idempotency_key, transaction_id, status)
    VALUES (%s, %s, 'PROCESSING')
    ON CONFLICT (idempotency_key) DO NOTHING
    RETURNING idempotency_key
"""

CHECK_IDEMPOTENCY = """
    SELECT status, response_payload, created_at
    FROM idempotency_store
    WHERE idempotency_key = %s
"""

UPDATE_IDEMPOTENCY_COMPLETED = """
    UPDATE idempotency_store
    SET status = 'COMPLETED',
        response_payload = %s
    WHERE idempotency_key = %s
"""

UPDATE_IDEMPOTENCY_FAILED = """
    UPDATE idempotency_store
    SET status = 'FAILED'
    WHERE idempotency_key = %s
"""

DELETE_IDEMPOTENCY = """
    DELETE FROM idempotency_store
    WHERE idempotency_key = %s
"""

DEDUCT_WALLET = """
    UPDATE user_wallets
    SET balance_cents = balance_cents - %s,
        updated_at = NOW()
    WHERE user_id = %s AND balance_cents >= %s
    RETURNING balance_cents
"""

CREDIT_WALLET = """
    UPDATE user_wallets
    SET balance_cents = balance_cents + %s,
        updated_at = NOW()
    WHERE user_id = %s
    RETURNING balance_cents
"""

INSERT_LEDGER_CHARGE = """
    INSERT INTO payment_ledger
    (transaction_id, user_id, event_id, operation, amount_cents, currency, status, description)
    VALUES (%s, %s, %s, 'CHARGE', %s, %s, 'SUCCESS', %s)
    RETURNING ledger_id
"""

INSERT_LEDGER_REFUND = """
    INSERT INTO payment_ledger
    (transaction_id, user_id, event_id, operation, amount_cents, currency, status, description)
    VALUES (%s, %s, %s, 'REFUND', %s, %s, 'SUCCESS', %s)
    RETURNING ledger_id
"""

GET_LEDGER_ENTRY = """
    SELECT ledger_id, amount_cents, user_id, status
    FROM payment_ledger
    WHERE ledger_id = %s AND operation = 'CHARGE' AND status = 'SUCCESS'
"""
