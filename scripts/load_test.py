import os
import random
import uuid

from locust import HttpUser, between, events, task

FLASH_SALE_EVENT_ID = os.environ.get(
    "FLASH_SALE_EVENT_ID",
    "11111111-1111-1111-1111-111111111111",
)
API_GATEWAY_URL = os.environ.get("API_GATEWAY_URL", "https://api.flash-sale.example.com")

TIERS = ["GENERAL", "VIP", "PLATINUM"]

IDEMPOTENCY_TEST_USER_ID = uuid.UUID("22222222-2222-2222-2222-222222222222")
COLLISION_COUNT = 0
MAX_COLLISIONS = 5


def random_user_id() -> str:
    return str(uuid.uuid4())


def random_quantity() -> int:
    return random.randint(1, 4)


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global COLLISION_COUNT
    COLLISION_COUNT = 0


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    if COLLISION_COUNT < MAX_COLLISIONS:
        environment.runner.log(f"Idempotency collision test: {COLLISION_COUNT}/{MAX_COLLISIONS} collisions sent")


class FlashSaleUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = API_GATEWAY_URL

    @task(weight=70)
    def buy_ticket(self):
        payload = {
            "user_id": random_user_id(),
            "event_id": FLASH_SALE_EVENT_ID,
            "tier_name": random.choice(TIERS),
            "quantity": 1,
        }
        with self.client.post("/buy-ticket", json=payload, catch_response=True) as resp:
            if resp.status_code == 202:
                resp.success()
                self.transaction_id = resp.json()["transaction_id"]
            elif resp.status_code == 409:
                resp.success()
            else:
                resp.failure(f"Unexpected status {resp.status_code}: {resp.text}")

    @task(weight=15)
    def buy_ticket_multi_quantity(self):
        payload = {
            "user_id": random_user_id(),
            "event_id": FLASH_SALE_EVENT_ID,
            "tier_name": random.choice(TIERS),
            "quantity": random_quantity(),
        }
        with self.client.post("/buy-ticket", json=payload, catch_response=True) as resp:
            if resp.status_code == 202:
                resp.success()
                self.transaction_id = resp.json()["transaction_id"]
            elif resp.status_code == 409:
                resp.success()
            else:
                resp.failure(f"Unexpected status {resp.status_code}: {resp.text}")

    @task(weight=10)
    def check_status(self):
        if hasattr(self, "transaction_id"):
            self.client.get(f"/status/{self.transaction_id}")

    @task(weight=5)
    def idempotency_collision_buy(self):
        global COLLISION_COUNT
        if COLLISION_COUNT >= MAX_COLLISIONS:
            return

        payload = {
            "user_id": str(IDEMPOTENCY_TEST_USER_ID),
            "event_id": FLASH_SALE_EVENT_ID,
            "tier_name": "VIP",
            "quantity": 1,
        }
        name = "buy_ticket_idempotency_collision"
        with self.client.post("/buy-ticket", json=payload, catch_response=True, name=name) as resp:
            if resp.status_code in (202, 409):
                resp.success()
                if resp.status_code == 202:
                    self.transaction_id = resp.json()["transaction_id"]
                COLLISION_COUNT += 1
            else:
                resp.failure(f"Idempotency collision unexpected status {resp.status_code}: {resp.text}")

    @task(weight=3)
    def buy_ticket_sold_out(self):
        payload = {
            "user_id": random_user_id(),
            "event_id": FLASH_SALE_EVENT_ID,
            "tier_name": random.choice(TIERS),
            "quantity": 1,
        }
        name = "buy_ticket_sold_out_event"
        with self.client.post("/buy-ticket", json=payload, catch_response=True, name=name) as resp:
            if resp.status_code in (202, 409, 404):
                resp.success()
            else:
                resp.failure(f"Sold-out test unexpected status {resp.status_code}: {resp.text}")

    @task(weight=3)
    def buy_ticket_invalid_payload(self):
        payload = {
            "user_id": "not-a-valid-uuid",
            "event_id": FLASH_SALE_EVENT_ID,
            "tier_name": "",
            "quantity": 0,
        }
        name = "buy_ticket_invalid_payload"
        with self.client.post("/buy-ticket", json=payload, catch_response=True, name=name) as resp:
            if resp.status_code == 422:
                resp.success()
            else:
                resp.failure(f"Invalid payload test unexpected status {resp.status_code}: {resp.text}")
