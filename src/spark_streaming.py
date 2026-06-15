import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

BASE_DIR = Path(__file__).resolve().parent.parent
LOG4J_CONFIG = BASE_DIR / "conf" / "log4j2.properties"

logging.getLogger("py4j").setLevel(logging.ERROR)

# =========================
# 1. SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile=file:/{LOG4J_CONFIG.as_posix()}") \
    .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile=file:/{LOG4J_CONFIG.as_posix()}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# 2. SCHEMAS
# =========================
schemas = {

# -------- DIMENSION --------
"customers": StructType([
    StructField("customer_id", IntegerType()),
    StructField("zip", IntegerType()),
    StructField("city", StringType()),
    StructField("signup_date", StringType()),
    StructField("gender", StringType()),
    StructField("age_group", StringType()),
    StructField("acquisition_channel", StringType()),
]),

"products": StructType([
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("segment", StringType()),
    StructField("size", StringType()),
    StructField("color", StringType()),
    StructField("price", FloatType()),
    StructField("cogs", FloatType()),
]),

"promotions": StructType([
    StructField("promo_id", StringType()),
    StructField("promo_name", StringType()),
    StructField("promo_type", StringType()),
    StructField("discount_value", FloatType()),
    StructField("start_date", StringType()),
    StructField("end_date", StringType()),
    StructField("applicable_category", StringType()),
    StructField("promo_channel", StringType()),
    StructField("stackable_flag", IntegerType()),
    StructField("min_order_value", FloatType()),
]),

"geography": StructType([
    StructField("zip", IntegerType()),
    StructField("city", StringType()),
    StructField("region", StringType()),
    StructField("district", StringType()),
]),


# -------- TRANSACTION --------
"orders": StructType([
    StructField("order_id", IntegerType()),
    StructField("order_date", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("zip", IntegerType()),
    StructField("order_status", StringType()),
    StructField("payment_method", StringType()),
    StructField("device_type", StringType()),
    StructField("order_source", StringType()),
]),

"order_items": StructType([
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", FloatType()),
    StructField("discount_amount", FloatType()),
    StructField("promo_id", StringType()),
    StructField("promo_id_2", StringType()),
]),

"payments": StructType([
    StructField("order_id", IntegerType()),
    StructField("payment_method", StringType()),
    StructField("payment_value", FloatType()),
    StructField("installments", IntegerType()),
]),

"shipments": StructType([
    StructField("order_id", IntegerType()),
    StructField("ship_date", StringType()),
    StructField("delivery_date", StringType()),
    StructField("shipping_fee", FloatType()),
]),

"returns": StructType([
    StructField("return_id", StringType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("return_date", StringType()),
    StructField("return_reason", StringType()),
    StructField("return_quantity", IntegerType()),
    StructField("refund_amount", FloatType()),
]),

"reviews": StructType([
    StructField("review_id", StringType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("review_date", StringType()),
    StructField("rating", IntegerType()),
    StructField("review_title", StringType()),
]),


# -------- ANALYTICS --------
"sales": StructType([
    StructField("date", StringType()),
    StructField("revenue", FloatType()),
    StructField("cogs", FloatType()),
]),

"inventory": StructType([
    StructField("snapshot_date", StringType()),
    StructField("product_id", IntegerType()),
    StructField("stock_on_hand", IntegerType()),
    StructField("units_received", IntegerType()),
    StructField("units_sold", IntegerType()),
    StructField("stockout_days", IntegerType()),
    StructField("days_of_supply", FloatType()),
    StructField("fill_rate", FloatType()),
    StructField("stockout_flag", IntegerType()),
    StructField("overstock_flag", IntegerType()),
    StructField("reorder_flag", IntegerType()),
    StructField("sell_through_rate", FloatType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("segment", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
]),

"web_traffic": StructType([
    StructField("date", StringType()),
    StructField("sessions", IntegerType()),
    StructField("unique_visitors", IntegerType()),
    StructField("page_views", IntegerType()),
    StructField("bounce_rate", FloatType()),
    StructField("avg_session_duration_sec", FloatType()),
    StructField("conversion_rate", FloatType()),
    StructField("traffic_source", StringType()),
]),
}


# =========================
# 3. READ KAFKA
# =========================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe",
        "customers,products,promotions,geography,"
        "orders,order_items,payments,shipments,returns,reviews,"
        "sales,inventory,web_traffic"
    ) \
    .option("startingOffsets", "earliest") \
    .load()


# =========================
# 4. PREP DATA
# =========================
raw_df = kafka_df.select(
    col("topic"),
    col("value").cast("string").alias("value")
)

console_query = raw_df.select("topic", "value").writeStream \
    .queryName("raw_kafka_console") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 50) \
    .start()


# =========================
# 5. ROUTING FUNCTION
# =========================
def parse_topic(df, topic):
    return df.filter(col("topic") == topic) \
        .select(
            from_json(col("value"), schemas[topic]).alias("data")
        ) \
        .filter(col("data").isNotNull()) \
        .select("data.*")


# =========================
# 6. STREAMS
# =========================
streams = {}

for topic in schemas.keys():
    streams[topic] = parse_topic(raw_df, topic)


# =========================
# 7. WRITE TO HDFS
# =========================
queries = []
queries.append(console_query)

for topic, df in streams.items():

    query = df.writeStream \
        .format("parquet") \
        .option("path", f"hdfs://localhost:9000/output_test/{topic}") \
        .option("checkpointLocation", f"hdfs://localhost:9000/checkpoint_test/{topic}") \
        .outputMode("append") \
        .start()

    queries.append(query)


# =========================
# 8. RUN FOREVER
# =========================
spark.streams.awaitAnyTermination()
