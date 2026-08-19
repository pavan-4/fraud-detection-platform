# producer/transaction_producer.py
#
# PURPOSE:
#   Simulates a card payment processing system sending transaction events to Kafka.
#   In real life, this would be JPMorgan's card authorisation system.
#   We fake it with realistic randomised data so we have something to process.
#
# HOW TO RUN:
#   pip install kafka-python faker
#   python producer/transaction_producer.py
#
# WHAT IT DOES:
#   Sends ~1 transaction per second to Kafka topic: raw.transactions
#   Every ~20th transaction is intentionally "suspicious" (fraud simulation)
#   Press Ctrl+C to stop

import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

# ── SETUP ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"   # Local Docker Kafka
TOPIC_NAME = "raw.transactions"
TRANSACTIONS_PER_SECOND = 2                  # How fast to send events
FRAUD_RATE = 0.05                            # 5% of transactions are suspicious

# ── REFERENCE DATA ────────────────────────────────────────────────────────────
# In production these come from databases. We hardcode small sets for the sim.

MERCHANTS = [
    {"id": "M001", "name": "Tesco Dublin",         "mcc": "5411", "country": "IE", "lat": 53.3498, "lon": -6.2603},
    {"id": "M002", "name": "Penneys Grafton St",   "mcc": "5651", "country": "IE", "lat": 53.3418, "lon": -6.2610},
    {"id": "M003", "name": "Boots Pharmacy",       "mcc": "5912", "country": "IE", "lat": 53.3441, "lon": -6.2675},
    {"id": "M004", "name": "Amazon Online",        "mcc": "5999", "country": "IE", "lat": None,    "lon": None},
    {"id": "M005", "name": "AIB ATM O Connell St", "mcc": "6011", "country": "IE", "lat": 53.3494, "lon": -6.2602},
    {"id": "M006", "name": "Marks & Spencer",      "mcc": "5311", "country": "GB", "lat": 51.5074, "lon": -0.1278},
    {"id": "M007", "name": "Paris Metro Shop",     "mcc": "5411", "country": "FR", "lat": 48.8566, "lon": 2.3522},
    {"id": "M008", "name": "Ryanair Online",       "mcc": "4511", "country": "IE", "lat": None,    "lon": None},
    {"id": "M009", "name": "Supermacs",            "mcc": "5812", "country": "IE", "lat": 53.2707, "lon": -9.0568},
    {"id": "M010", "name": "Shell Petrol Station", "mcc": "5541", "country": "IE", "lat": 53.3999, "lon": -6.2000},
]

CURRENCIES = {
    "IE": "EUR", "FR": "EUR", "DE": "EUR",
    "GB": "GBP",
    "US": "USD",
}

FX_TO_EUR = {
    "EUR": 1.0,
    "GBP": 1.17,
    "USD": 0.92,
}

# Simulated card pool — 50 fake cards
CARDS = [
    {"card_id": f"CARD_{i:04d}", "customer_id": f"CUST_{i:04d}", "home_country": "IE"}
    for i in range(1, 51)
]

TERMINAL_TYPES = ["POS", "ATM", "ONLINE", "CONTACTLESS"]
TERMINAL_WEIGHTS = [0.4, 0.1, 0.25, 0.25]   # CONTACTLESS and POS dominate


# ── TRANSACTION GENERATOR ─────────────────────────────────────────────────────

def generate_normal_transaction() -> dict:
    """Generate a realistic, legitimate-looking transaction."""
    card = random.choice(CARDS)
    merchant = random.choice(MERCHANTS)
    currency = CURRENCIES.get(merchant["country"], "EUR")
    amount_local = round(random.uniform(1.50, 350.00), 2)
    amount_eur = round(amount_local * FX_TO_EUR.get(currency, 1.0), 2)

    # Terminal type logic: online merchants have ONLINE terminal
    if merchant["lat"] is None:
        terminal_type = "ONLINE"
        ip_address = fake.ipv4()
        lat, lon = None, None
    else:
        terminal_type = random.choices(
            ["POS", "ATM", "CONTACTLESS"],
            weights=[0.4, 0.1, 0.5]
        )[0]
        ip_address = None
        lat = merchant["lat"] + random.uniform(-0.001, 0.001)   # Slight GPS noise
        lon = merchant["lon"] + random.uniform(-0.001, 0.001)

    return {
        "transaction_id":        str(uuid.uuid4()),
        "card_id":               card["card_id"],
        "customer_id":           card["customer_id"],
        "merchant_id":           merchant["id"],
        "merchant_name":         merchant["name"],
        "merchant_category_code": merchant["mcc"],
        "amount_local":          amount_local,
        "currency_code":         currency,
        "amount_eur":            amount_eur if currency != "EUR" else None,
        "country_code":          merchant["country"],
        "terminal_type":         terminal_type,
        "event_timestamp":       int(datetime.now(timezone.utc).timestamp() * 1000),
        "ip_address":            ip_address,
        "latitude":              lat,
        "longitude":             lon,
    }


def generate_suspicious_transaction() -> dict:
    """
    Generate a transaction with fraud signals baked in.
    These are the patterns our Silver job + ML model should catch.
    """
    card = random.choice(CARDS)
    fraud_type = random.choice([
        "high_amount",        # Unusually large amount
        "foreign_country",    # Card used in unexpected country
        "rapid_succession",   # Sent immediately after a normal txn
        "suspicious_mcc",     # High-risk merchant category
    ])

    txn = generate_normal_transaction()
    txn["card_id"] = card["card_id"]
    txn["customer_id"] = card["customer_id"]
    txn["transaction_id"] = str(uuid.uuid4())   # New unique ID

    if fraud_type == "high_amount":
        txn["amount_local"] = round(random.uniform(2000, 9999), 2)
        txn["amount_eur"] = txn["amount_local"]

    elif fraud_type == "foreign_country":
        txn["country_code"] = random.choice(["NG", "RO", "UA", "BR"])
        txn["currency_code"] = "USD"
        txn["amount_eur"] = round(txn["amount_local"] * 0.92, 2)
        txn["latitude"] = random.uniform(-90, 90)
        txn["longitude"] = random.uniform(-180, 180)

    elif fraud_type == "suspicious_mcc":
        txn["merchant_category_code"] = random.choice(["7995", "5933", "6051"])
        # 7995 = Gambling, 5933 = Pawn Shops, 6051 = Non-financial institutions

    # Log the fraud type for our own debugging
    txn["_fraud_simulation_type"] = fraud_type   # Will be visible in Kafka UI

    return txn


# ── KAFKA PRODUCER ────────────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer with JSON serialisation."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        # Reliability settings
        acks="all",                  # Wait for all replicas to acknowledge
        retries=3,
        retry_backoff_ms=500,
    )


def on_send_success(record_metadata):
    """Callback — called when message is successfully delivered."""
    logger.info(
        f"✅ Sent → topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(exception):
    """Callback — called when message delivery fails."""
    logger.error(f"❌ Failed to send message: {exception}")


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────

def main():
    logger.info("🚀 Starting Fraud Detection Transaction Producer")
    logger.info(f"   Kafka:  {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"   Topic:  {TOPIC_NAME}")
    logger.info(f"   Rate:   {TRANSACTIONS_PER_SECOND} txn/sec")
    logger.info(f"   Fraud:  {FRAUD_RATE * 100:.0f}% of transactions are suspicious")
    logger.info("   Press Ctrl+C to stop\n")

    producer = create_producer()
    total_sent = 0
    total_fraud = 0

    try:
        while True:
            # Decide: normal or suspicious transaction?
            is_fraud = random.random() < FRAUD_RATE

            if is_fraud:
                txn = generate_suspicious_transaction()
                total_fraud += 1
                logger.warning(
                    f"🚨 SUSPICIOUS | card={txn['card_id']} "
                    f"amount=€{txn['amount_local']:.2f} "
                    f"country={txn['country_code']} "
                    f"type={txn.get('_fraud_simulation_type', 'unknown')}"
                )
            else:
                txn = generate_normal_transaction()
                logger.info(
                    f"💳 Normal     | card={txn['card_id']} "
                    f"merchant={txn['merchant_name']} "
                    f"amount=€{txn['amount_local']:.2f}"
                )

            # Send to Kafka
            # Key = card_id ensures all transactions for one card
            # go to the same partition (important for ordering guarantees)
            producer.send(
                topic=TOPIC_NAME,
                key=txn["card_id"],
                value=txn
            ).add_callback(on_send_success).add_errback(on_send_error)

            total_sent += 1

            # Print summary every 20 transactions
            if total_sent % 20 == 0:
                logger.info(
                    f"\n📊 Summary: {total_sent} sent | "
                    f"{total_fraud} suspicious ({total_fraud/total_sent*100:.1f}%)\n"
                )

            # Control the rate
            time.sleep(1 / TRANSACTIONS_PER_SECOND)

    except KeyboardInterrupt:
        logger.info(f"\n⏹️  Stopped. Total sent: {total_sent} | Suspicious: {total_fraud}")
    finally:
        producer.flush()    # Ensure all buffered messages are sent
        producer.close()
        logger.info("Producer closed cleanly.")


if __name__ == "__main__":
    main()
