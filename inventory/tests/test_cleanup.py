from unittest.mock import MagicMock

import pytest
from inventory.cleanup import ttl_cleanup_handler


@pytest.fixture
def mock_db():
    db = MagicMock()
    return db


class TestTtlCleanupHandler:
    def test_restore_quantity_on_ttl_removal(self, mock_db):
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "userIdentity": {
                        "type": "Service",
                        "principalId": "dynamodb.amazonaws.com",
                    },
                    "dynamodb": {
                        "OldImage": {
                            "entity_type": {"S": "RESERVATION"},
                            "event_id": {"S": "evt-123"},
                            "tier_name": {"S": "VIP"},
                            "quantity": {"N": "2"},
                        }
                    },
                }
            ]
        }

        ttl_cleanup_handler(event=event, context=None, db=mock_db)

        mock_db.restore_quantity.assert_called_once_with("evt-123", "VIP", 2)

    def test_ignore_non_remove_events(self, mock_db):
        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "userIdentity": {
                        "type": "Service",
                        "principalId": "dynamodb.amazonaws.com",
                    },
                    "dynamodb": {
                        "OldImage": {
                            "entity_type": {"S": "RESERVATION"},
                            "event_id": {"S": "evt-123"},
                            "tier_name": {"S": "VIP"},
                            "quantity": {"N": "2"},
                        }
                    },
                }
            ]
        }

        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        mock_db.restore_quantity.assert_not_called()

    def test_ignore_non_service_identity(self, mock_db):
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "userIdentity": {
                        "type": "User",
                        "principalId": "some-user",
                    },
                    "dynamodb": {
                        "OldImage": {
                            "entity_type": {"S": "RESERVATION"},
                            "event_id": {"S": "evt-123"},
                            "tier_name": {"S": "VIP"},
                            "quantity": {"N": "2"},
                        }
                    },
                }
            ]
        }

        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        mock_db.restore_quantity.assert_not_called()

    def test_ignore_non_dynamodb_service(self, mock_db):
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "userIdentity": {
                        "type": "Service",
                        "principalId": "lambda.amazonaws.com",
                    },
                    "dynamodb": {
                        "OldImage": {
                            "entity_type": {"S": "RESERVATION"},
                            "event_id": {"S": "evt-123"},
                            "tier_name": {"S": "VIP"},
                            "quantity": {"N": "2"},
                        }
                    },
                }
            ]
        }

        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        mock_db.restore_quantity.assert_not_called()

    def test_ignore_non_reservation_entity(self, mock_db):
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "userIdentity": {
                        "type": "Service",
                        "principalId": "dynamodb.amazonaws.com",
                    },
                    "dynamodb": {
                        "OldImage": {
                            "entity_type": {"S": "EVENT"},
                            "event_id": {"S": "evt-123"},
                        }
                    },
                }
            ]
        }

        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        mock_db.restore_quantity.assert_not_called()

    def test_ignore_missing_old_image(self, mock_db):
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "userIdentity": {
                        "type": "Service",
                        "principalId": "dynamodb.amazonaws.com",
                    },
                    "dynamodb": {},
                }
            ]
        }

        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        mock_db.restore_quantity.assert_not_called()

    def test_ignore_missing_required_fields(self, mock_db):
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "userIdentity": {
                        "type": "Service",
                        "principalId": "dynamodb.amazonaws.com",
                    },
                    "dynamodb": {
                        "OldImage": {
                            "entity_type": {"S": "RESERVATION"},
                        }
                    },
                }
            ]
        }

        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        mock_db.restore_quantity.assert_not_called()

    def test_process_multiple_records(self, mock_db):
        event = {
            "Records": [
                {
                    "eventName": "REMOVE",
                    "userIdentity": {
                        "type": "Service",
                        "principalId": "dynamodb.amazonaws.com",
                    },
                    "dynamodb": {
                        "OldImage": {
                            "entity_type": {"S": "RESERVATION"},
                            "event_id": {"S": "evt-123"},
                            "tier_name": {"S": "VIP"},
                            "quantity": {"N": "2"},
                        }
                    },
                },
                {
                    "eventName": "REMOVE",
                    "userIdentity": {
                        "type": "Service",
                        "principalId": "dynamodb.amazonaws.com",
                    },
                    "dynamodb": {
                        "OldImage": {
                            "entity_type": {"S": "RESERVATION"},
                            "event_id": {"S": "evt-456"},
                            "tier_name": {"S": "GENERAL"},
                            "quantity": {"N": "5"},
                        }
                    },
                },
            ]
        }

        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        assert mock_db.restore_quantity.call_count == 2
        mock_db.restore_quantity.assert_any_call("evt-123", "VIP", 2)
        mock_db.restore_quantity.assert_any_call("evt-456", "GENERAL", 5)

    def test_empty_records(self, mock_db):
        event: dict = {"Records": []}
        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        mock_db.restore_quantity.assert_not_called()

    def test_missing_records_key(self, mock_db):
        event: dict = {}
        ttl_cleanup_handler(event=event, context=None, db=mock_db)
        mock_db.restore_quantity.assert_not_called()
