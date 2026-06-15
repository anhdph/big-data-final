# Big Data - E-commerce Analytics

Repository cho tiểu luận cuối kỳ môn Big Data. Dự án xây dựng pipeline phân tích dữ liệu thương mại điện tử bằng Hadoop HDFS, Apache Spark, Spark SQL, Spark Structured Streaming, Kafka và Spark MLlib.

## Thành viên

- Hoàng Thụy Hồng Ân
- Dương Phương Anh
- Huỳnh Thụy Mai Nguyên
- Trần Khánh Ngân

## Mục tiêu

Dự án mô phỏng hệ thống phân tích dữ liệu e-commerce với 3 phần chính:

1. Batch analytics bằng Spark SQL trên dữ liệu CSV lưu trong HDFS.
2. Streaming pipeline đọc dữ liệu từ Kafka, parse theo schema từng topic và ghi Parquet xuống HDFS.
3. MLlib model dự đoán `net_unit_sold`, sau đó thử các kịch bản điều chỉnh giá để tối ưu doanh thu.

## Dataset

Dataset gốc được lưu trên Google Drive:

https://drive.google.com/drive/folders/11X0X-UE6LaP96I6LqUsk1kqTukuwuxPi?usp=sharing

Sau khi tải dữ liệu, đặt các file CSV vào thư mục `data/` ở root project. Thư mục này không commit raw data lên Git, chỉ giữ `.gitkeep`.

Các bảng dữ liệu được sử dụng:

- `customers.csv`
- `geography.csv`
- `inventory.csv`
- `order_items.csv`
- `orders.csv`
- `payments.csv`
- `products.csv`
- `promotions.csv`
- `returns.csv`
- `reviews.csv`
- `sales.csv`
- `shipments.csv`
- `web_traffic.csv`

## Cấu trúc repository

```text
.
|-- command/
|   |-- deployment_environment.txt      # Log phiên bản Java, Hadoop, Python, Spark
|   |-- hadoop_setup.txt                # Ghi chú setup Hadoop/YARN trên Windows
|   |-- hdfs_data_read_and_write.txt    # Ví dụ đọc/ghi dữ liệu HDFS bằng PySpark
|   `-- hdfs_data_upload.txt            # Lệnh upload CSV lên HDFS
|-- conf/
|   `-- log4j2.properties               # Cấu hình giảm log khi chạy Spark Streaming
|-- data/
|   `-- .gitkeep                        # Nơi đặt raw CSV sau khi tải dataset
|-- mllib_plot_result/                  # Biểu đồ kết quả MLlib
|-- sql_data_output/                    # CSV output từ Spark SQL
|-- src/
|   |-- kafka_producer.py               # Producer gửi từng CSV vào Kafka topic tương ứng
|   |-- spark_streaming.py              # Spark Structured Streaming: Kafka -> HDFS Parquet
|   |-- sparksql_ecommerce.ipynb        # Phân tích Spark SQL
|   |-- mllib_unit_sold_and_price_optimization.ipynb
|   `-- test_sql.ipynb                  # Kiểm tra dữ liệu Parquet output từ streaming
`-- README.md
```

## Môi trường đã dùng

Theo log trong `command/deployment_environment.txt`:

- Windows 10
- OpenJDK 17.0.18
- Hadoop 3.4.3
- Apache Spark 4.1.1, Scala 2.13.17
- Python 3.14.4
- Kafka chạy local tại `localhost:9092`
- HDFS NameNode tại `hdfs://localhost:9000`

Python packages chính:

```powershell
pip install pandas kafka-python pyspark matplotlib seaborn numpy
```

Khi chạy Spark Structured Streaming với Kafka, cần đảm bảo Spark có package Kafka connector tương ứng, ví dụ với Spark 4.1.1/Scala 2.13:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 src\spark_streaming.py
```

## Chuẩn bị dữ liệu trên HDFS

Tạo thư mục HDFS và upload dữ liệu:

```powershell
hdfs dfs -mkdir -p /bigdata/nhom11/data
hdfs dfs -put data\*.csv /bigdata/nhom11/data
hdfs dfs -ls /bigdata/nhom11/data
```

Notebook `src/sparksql_ecommerce.ipynb` đang đọc dữ liệu từ:

```text
hdfs://localhost:9000/bigdata/nhom11/data
```

Nếu upload vào path khác, cần chỉnh biến `data_path` trong notebook.

## Chạy Spark SQL analytics

Mở và chạy notebook:

```text
src/sparksql_ecommerce.ipynb
```

Notebook tạo temp view cho 13 bảng dữ liệu và chạy các nhóm phân tích như:

- Doanh thu, COGS, gross profit theo tháng.
- Phân phối khách hàng theo kênh acquisition.
- Top sản phẩm theo biên lợi nhuận.
- Rating sản phẩm theo danh mục.
- Web traffic theo nguồn truy cập.
- Khách hàng theo vùng địa lý.
- Hiệu quả khuyến mãi.
- RFM analysis.
- Cohort retention.
- Inventory health.
- Funnel traffic-to-revenue.
- Promotion ROI/lift.
- Delivery performance.
- Return analysis.
- Payment intelligence.

Kết quả CSV đã export nằm trong `sql_data_output/`, gồm `Q01` đến `Q18` trừ `Q13`.
Kết quả chụp màn hình các câu truy cấn nằm trong `sql_code_result`, gồm `Q01` đến `Q18` trừ `Q13`.

## Chạy streaming Kafka -> HDFS

### 1. Khởi động Hadoop/HDFS

Ví dụ trên Windows:

```powershell
cd E:\Hadoop\hadoop-3.4.3\sbin
start-dfs.cmd
start-yarn.cmd
jps
```

Các process tối thiểu cần thấy: `NameNode`, `DataNode`, `ResourceManager`, `NodeManager`.

### 2. Khởi động Kafka

Kafka cần chạy tại:

```text
localhost:9092
```

Producer sẽ gửi vào các topic cùng tên với file CSV:

```text
customers, geography, inventory, order_items, orders, payments,
products, promotions, returns, reviews, sales, shipments, web_traffic
```

### 3. Chạy Spark Streaming consumer

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 src\spark_streaming.py
```

Script sẽ:

- Subscribe toàn bộ 13 topic.
- Parse JSON theo schema tương ứng.
- Ghi dữ liệu Parquet vào `hdfs://localhost:9000/output_test/<topic>`.
- Ghi checkpoint vào `hdfs://localhost:9000/checkpoint_test/<topic>`.
- In raw Kafka message ra console để kiểm tra nhanh.

### 4. Chạy Kafka producer

Mở terminal khác và chạy:

```powershell
python src\kafka_producer.py
```

Producer đọc từng file trong `data/`, chuẩn hóa key về chữ thường, thêm trường `source_topic`, rồi gửi luân phiên từng dòng vào Kafka.

## Kiểm tra dữ liệu streaming output

Mở notebook:

```text
src/test_sql.ipynb
```

Notebook đọc dữ liệu Parquet từ:

```text
hdfs://localhost:9000/output_test/<topic>
```

và kiểm tra count/schema/truy vấn SQL thử trên các topic đã stream.

## MLlib: dự đoán lượng bán và tối ưu giá

Notebook:

```text
src/mllib_unit_sold_and_price_optimization.ipynb
```

Luồng xử lý chính:

1. Đọc các bảng customers, geography, inventory, order_items, orders, products, returns và web_traffic.
2. Tổng hợp dữ liệu theo `category`, `segment`, `year`, `month`.
3. Tạo target `net_unit_sold = units_sold - total_return`.
4. Tạo lag features, rolling average 3 tháng, thông tin promotion, traffic, tồn kho và giá.
5. Train Random Forest Regressor bằng Spark MLlib.
6. Đánh giá model trên tập test.
7. Sinh price scenarios từ -50% đến 0% và chọn kịch bản doanh thu cao nhất theo category/segment/tháng.

Kết quả model trong notebook:

```text
RMSE: 143.977
MAE: 79.941
R2: 0.845
```

Biểu đồ kết quả được lưu trong `mllib_plot_result/`, gồm:

- `unitsold_casual.png`
- `unitsold_genz.png`
- `unitsold_outdoor.png`
- `unitsold_streetwear.png`
- `revenue_casual.png`
- `revenue_genz.png`
- `revenue_outdoor.png`
- `revenue_streetwear.png`

## Ghi chú

- `data/` bị ignore để tránh commit raw dataset dung lượng lớn.
- Các file trong `command/` là log/lệnh tham khảo từ quá trình setup và chạy thử.
- Một số notebook có path tuyệt đối hoặc path theo môi trường cá nhân; khi chạy trên máy khác cần kiểm tra lại `data_path` hoặc biến output path.
- Nếu chạy lại streaming nhiều lần, có thể cần xóa hoặc đổi path `output_test` và `checkpoint_test` trên HDFS để tránh đọc lại checkpoint cũ.
