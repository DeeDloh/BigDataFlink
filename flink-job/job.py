import json
import logging
import os
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction, RuntimeContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lab3-flink")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mock-data")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "lab3")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def to_int(value):
    try:
        return int(value) if value not in (None, "") else None
    except (ValueError, TypeError):
        return None


def to_decimal(value):
    try:
        return Decimal(str(value)) if value not in (None, "") else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def to_date(value):
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    return None


class StarSchemaWriter(MapFunction):
    """Обрабатывает одно JSON-сообщение и раскладывает его по таблицам звезды."""

    def open(self, runtime_context: RuntimeContext):
        import psycopg2
        self.conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        self.conn.autocommit = False
        log.info("Connected to PostgreSQL")

    def close(self):
        if getattr(self, "conn", None):
            self.conn.close()

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
            self.write_record(record)
            self.conn.commit()
            return f"ok:{record.get('source_id', record.get('id'))}"
        except Exception as exc:
            self.conn.rollback()
            log.exception("Failed record: %s", exc)
            return "error"

    def one_value(self, sql, params):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None

    def get_or_insert_simple(self, table, id_col, value_col, value):
        if not value:
            return None
        result = self.one_value(f"SELECT {id_col} FROM {table} WHERE {value_col} = %s", (value,))
        if result:
            return result
        return self.one_value(
            f"INSERT INTO {table} ({value_col}) VALUES (%s) RETURNING {id_col}",
            (value,),
        )

    def get_or_insert_location(self, r):
        values = (r.get("store_location"), r.get("store_city"), r.get("store_state"), r.get("store_country"))
        loc_id = self.one_value(
            """
            SELECT location_id FROM dim_locations
            WHERE location IS NOT DISTINCT FROM %s
              AND city IS NOT DISTINCT FROM %s
              AND state IS NOT DISTINCT FROM %s
              AND country IS NOT DISTINCT FROM %s
            """,
            values,
        )
        if loc_id:
            return loc_id
        return self.one_value(
            """
            INSERT INTO dim_locations (location, city, state, country)
            VALUES (%s, %s, %s, %s)
            RETURNING location_id
            """,
            values,
        )

    def get_or_insert_pet(self, r):
        values = (r.get("customer_pet_type"), r.get("customer_pet_name"), r.get("customer_pet_breed"))
        pet_id = self.one_value(
            """
            SELECT pet_id FROM dim_pets
            WHERE pet_type IS NOT DISTINCT FROM %s
              AND pet_name IS NOT DISTINCT FROM %s
              AND pet_breed IS NOT DISTINCT FROM %s
            """,
            values,
        )
        if pet_id:
            return pet_id
        return self.one_value(
            "INSERT INTO dim_pets (pet_type, pet_name, pet_breed) VALUES (%s, %s, %s) RETURNING pet_id",
            values,
        )

    def upsert_customer(self, r, pet_id):
        return self.one_value(
            """
            INSERT INTO dim_customers (first_name, last_name, age, email, country, postal_code, pet_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                age = EXCLUDED.age,
                country = EXCLUDED.country,
                postal_code = EXCLUDED.postal_code,
                pet_id = EXCLUDED.pet_id
            RETURNING customer_id
            """,
            (
                r.get("customer_first_name"), r.get("customer_last_name"), to_int(r.get("customer_age")),
                r.get("customer_email"), r.get("customer_country"), r.get("customer_postal_code"), pet_id,
            ),
        )

    def upsert_seller(self, r):
        return self.one_value(
            """
            INSERT INTO dim_sellers (first_name, last_name, email, country, postal_code)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                country = EXCLUDED.country,
                postal_code = EXCLUDED.postal_code
            RETURNING seller_id
            """,
            (r.get("seller_first_name"), r.get("seller_last_name"), r.get("seller_email"), r.get("seller_country"), r.get("seller_postal_code")),
        )

    def get_or_insert_store(self, r, location_id):
        values = (r.get("store_name"), location_id, r.get("store_phone"), r.get("store_email"))
        store_id = self.one_value(
            """
            SELECT store_id FROM dim_stores
            WHERE name IS NOT DISTINCT FROM %s
              AND location_id IS NOT DISTINCT FROM %s
              AND phone IS NOT DISTINCT FROM %s
              AND email IS NOT DISTINCT FROM %s
            """,
            values,
        )
        if store_id:
            return store_id
        return self.one_value(
            "INSERT INTO dim_stores (name, location_id, phone, email) VALUES (%s, %s, %s, %s) RETURNING store_id",
            values,
        )

    def get_or_insert_supplier(self, r):
        values = (r.get("supplier_name"), r.get("supplier_email"), r.get("supplier_phone"))
        supplier_id = self.one_value(
            """
            SELECT supplier_id FROM dim_suppliers
            WHERE name IS NOT DISTINCT FROM %s
              AND email IS NOT DISTINCT FROM %s
              AND phone IS NOT DISTINCT FROM %s
            """,
            values,
        )
        if supplier_id:
            return supplier_id
        return self.one_value(
            """
            INSERT INTO dim_suppliers (name, contact, email, phone, address, city, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING supplier_id
            """,
            (
                r.get("supplier_name"), r.get("supplier_contact"), r.get("supplier_email"),
                r.get("supplier_phone"), r.get("supplier_address"), r.get("supplier_city"), r.get("supplier_country"),
            ),
        )

    def upsert_date(self, sale_date):
        d = to_date(sale_date)
        if d is None:
            return None
        return self.one_value(
            """
            INSERT INTO dim_dates (date_id, day, month, year, quarter, day_of_week, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_id) DO UPDATE SET date_id = EXCLUDED.date_id
            RETURNING date_id
            """,
            (d, d.day, d.month, d.year, (d.month - 1) // 3 + 1, d.isoweekday(), d.weekday() >= 5),
        )

    def get_or_insert_product(self, r):
        category_id = self.get_or_insert_simple("dim_categories", "category_id", "category_name", r.get("product_category"))
        brand_id = self.get_or_insert_simple("dim_brands", "brand_id", "brand_name", r.get("product_brand"))
        material_id = self.get_or_insert_simple("dim_materials", "material_id", "material_name", r.get("product_material"))
        color_id = self.get_or_insert_simple("dim_colors", "color_id", "color_name", r.get("product_color"))
        size_id = self.get_or_insert_simple("dim_sizes", "size_id", "size_name", r.get("product_size"))

        key = (r.get("product_name"), category_id, to_decimal(r.get("product_price")), color_id, size_id, brand_id, material_id)
        product_id = self.one_value(
            """
            SELECT product_id FROM dim_products
            WHERE name IS NOT DISTINCT FROM %s
              AND category_id IS NOT DISTINCT FROM %s
              AND price IS NOT DISTINCT FROM %s
              AND color_id IS NOT DISTINCT FROM %s
              AND size_id IS NOT DISTINCT FROM %s
              AND brand_id IS NOT DISTINCT FROM %s
              AND material_id IS NOT DISTINCT FROM %s
            """,
            key,
        )
        if product_id:
            return product_id
        return self.one_value(
            """
            INSERT INTO dim_products
                (name, category_id, price, weight, color_id, size_id, brand_id, material_id,
                 description, rating, reviews, release_date, expiry_date, pet_category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING product_id
            """,
            (
                r.get("product_name"), category_id, to_decimal(r.get("product_price")), to_decimal(r.get("product_weight")),
                color_id, size_id, brand_id, material_id, r.get("product_description"), to_decimal(r.get("product_rating")),
                to_int(r.get("product_reviews")), to_date(r.get("product_release_date")), to_date(r.get("product_expiry_date")),
                r.get("pet_category"),
            ),
        )

    def write_record(self, r):
        pet_id = self.get_or_insert_pet(r)
        customer_id = self.upsert_customer(r, pet_id)
        seller_id = self.upsert_seller(r)
        location_id = self.get_or_insert_location(r)
        store_id = self.get_or_insert_store(r, location_id)
        supplier_id = self.get_or_insert_supplier(r)
        date_id = self.upsert_date(r.get("sale_date"))
        product_id = self.get_or_insert_product(r)

        source_id = r.get("source_id") or str(r.get("id"))
        self.one_value(
            """
            INSERT INTO fact_sales
                (source_id, date_id, customer_id, seller_id, product_id, store_id, supplier_id, quantity, total_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id) DO UPDATE SET
                date_id = EXCLUDED.date_id,
                customer_id = EXCLUDED.customer_id,
                seller_id = EXCLUDED.seller_id,
                product_id = EXCLUDED.product_id,
                store_id = EXCLUDED.store_id,
                supplier_id = EXCLUDED.supplier_id,
                quantity = EXCLUDED.quantity,
                total_price = EXCLUDED.total_price
            RETURNING fact_id
            """,
            (
                source_id, date_id, customer_id, seller_id, product_id, store_id, supplier_id,
                to_int(r.get("sale_quantity")), to_decimal(r.get("sale_total_price")),
            ),
        )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("kirill-lab3-flink")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source").map(StarSchemaWriter()).print()
    env.execute("Kirill Lab3 Kafka to PostgreSQL Star Schema")


if __name__ == "__main__":
    main()
