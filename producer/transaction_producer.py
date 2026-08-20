# producer/transaction_producer.py
#
# PURPOSE:
#   Simulates a card payment processing system sending transaction events to Kafka.
#   In real life, this would be JPMorgan's card authorisation system.
#   We fake it with realistic randomised data so we have something to process.
#
# HOW TO RUN:
#   pip install confluent-kafka faker python-dotenv
#   python producer/transaction_producer.py
#
# WHAT IT DOES:
#   Sends ~2 transactions per second to Azure Event Hubs topic: raw.transactions
#   Every ~20th transaction is intentionally "suspicious" (fraud simulation)
#   Press Ctrl+C to stop

import os
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer
from faker import Faker

load_dotenv()   # Reads .env file from project root

# ── SETUP ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

fake = Faker()

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
EVENT_HUBS_CONNECTION_STRING = os.getenv("EVENT_HUBS_CONNECTION_STRING")
EVENT_HUBS_NAMESPACE         = "fraud-platform-eh.servicebus.windows.net:9093"
TOPIC_NAME                   = "raw.transactions"
TRANSACTIONS_PER_SECOND      = 2        # How fast to send events
FRAUD_RATE                   = 0.05     # 5% of transactions are suspicious

# ── REFERENCE DATA ────────────────────────────────────────────────────────────
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


# ── TRANSACTION GENERATOR ─────────────────────────────────────────────────────

def generate_normal_transaction() -> dict:
    """Generate a realistic, legitimate-looking transaction."""
    card = random.choice(CARDS)
    merchant = random.choice(MERCHANTS)
    currency = CURRENCIES.get(merchant["country"], "EUR")
    amount_local = round(random.uniform(1.50, 350.00), 2)
    amount_eur = round(amount_local * FX_TO_EUR.get(currency, 1.0), 2)

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
        lat = merchant["lat"] + random.uniform(-0.001, 0.001)
        lon = merchant["lon"] + random.uniform(-0.001, 0.001)

    return {
        "transaction_id":         str(uuid.uuid4()),
        "card_id":                card["card_id"],
        "customer_id":            card["customer_id"],
        "merchant_id":            merchant["id"],
        "merchant_name":          merchant["name"],
        "merchant_category_code": merchant["mcc"],
        "amount_local":           amount_local,
        "currency_code":          currency,
        "amount_eur":             amount_eur if currency != "EUR" else None,
        "country_code":           merchant["country"],
        "terminal_type":          terminal_type,
        "event_timestamp":        int(datetime.now(timezone.utc).timestamp() * 1000),
        "ip_address":             ip_address,
        "latitude":               lat,
        "longitude":              lon,
    }


def generate_suspicious_transaction() -> dict:
    """Generate a transaction with fraud signals baked in."""
    card = random.choice(CARDS)
    fraud_type = random.choice([
        "high_amount",
        "foreign_country",
        "rapid_succession",
        "suspicious_mcc",
    ])

    txn = generate_normal_transaction()
    txn["card_id"] = card["card_id"]
    txn["customer_id"] = card["customer_id"]
    txn["transaction_id"] = str(uuid.uuid4())

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

    txn["_fraud_simulation_type"] = fraud_type
    return txn


# ── CONFLUENT KAFKA PRODUCER (Azure Event Hubs) ───────────────────────────────

def create_producer() -> Producer:
    """
    Create and return a confluent-kafka Producer connected to Azure Event Hubs.

    confluent-kafka is built on librdkafka which has first-class support for
    Azure Event Hubs SASL_SSL authentication — unlike kafka-python which has
    a known SASL handshake incompatibility with Event Hubs.
    """
    if not EVENT_HUBS_CONNECTION_STRING:
        raise ValueError(
            "EVENT_HUBS_CONNECTION_STRING not found. "
            "Make sure .env file exists in project root."
        )

    conf = {
        "bootstrap.servers":  EVENT_HUBS_NAMESPACE,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanism":     "PLAIN",
        "sasl.username":      "$ConnectionString",
        "sasl.password":      EVENT_HUBS_CONNECTION_STRING,
        "client.id":          "fraud-platform-producer",
        "acks":               "all",
        "retries":            3,
        "retry.backoff.ms":   500,
    }
    return Producer(conf)


def delivery_callback(err, msg):
    """
    Called once per message when delivery is confirmed or fails.
    confluent-kafka uses a single callback with err=None on success.
    """
    if err:
        logger.error(f"❌ Failed to send message: {err}")
    else:
        logger.info(
            f"✅ Sent → topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


# ── MAIN LOOP ─────────────────────────────────────────────────────────────────

def main():
    logger.info("🚀 Starting Fraud Detection Transaction Producer")
    logger.info(f"   Kafka:  {EVENT_HUBS_NAMESPACE}")
    logger.info(f"   Topic:  {TOPIC_NAME}")
    logger.info(f"   Rate:   {TRANSACTIONS_PER_SECOND} txn/sec")
    logger.info(f"   Fraud:  {FRAUD_RATE * 100:.0f}% of transactions are suspicious")
    logger.info("   Press Ctrl+C to stop\n")

    producer = create_producer()
    total_sent = 0
    total_fraud = 0

    try:
        while True:
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

            # Produce message to Event Hubs
            # Key = card_id ensures ordering per card within a partition
            producer.produce(
                topic=TOPIC_NAME,
                key=txn["card_id"].encode("utf-8"),
                value=json.dumps(txn).encode("utf-8"),
                callback=delivery_callback
            )

            # confluent-kafka buffers messages — poll() triggers callbacks
            # and flushes the internal queue without blocking
            producer.poll(0)

            total_sent += 1

            if total_sent % 20 == 0:
                logger.info(
                    f"\n📊 Summary: {total_sent} sent | "
                    f"{total_fraud} suspicious ({total_fraud/total_sent*100:.1f}%)\n"
                )

            time.sleep(1 / TRANSACTIONS_PER_SECOND)

    except KeyboardInterrupt:
        logger.info(f"\n⏹️  Stopped. Total sent: {total_sent} | Suspicious: {total_fraud}")
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()    # Block until all buffered messages are delivered
        logger.info("Producer closed cleanly.")


if __name__ == "__main__":
    main()