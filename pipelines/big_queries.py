def get_stg_query(source_raw_table: str, target_stg_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{target_stg_table}` AS
    SELECT
      CAST(InvoiceNo AS STRING) AS invoice_no,
      CAST(StockCode AS STRING) AS stock_code,
      TRIM(Description) AS description,
      CAST(Quantity AS INT64) AS quantity,
      CAST(UnitPrice AS FLOAT64) AS unit_price,
      InvoiceDate AS invoice_timestamp,
      CustomerID,
      Country,
      Quantity * UnitPrice AS total_sales
    FROM `{source_raw_table}`
    WHERE Quantity > 0
      AND UnitPrice > 0
    """


def get_fact_query(source_stg_table: str, target_fact_table: str) -> str:
    return f"""
    CREATE OR REPLACE TABLE `{target_fact_table}` AS
    SELECT
      DATE(invoice_timestamp) AS order_date,
      Country,
      COUNT(DISTINCT invoice_no) AS total_orders,
      SUM(quantity) AS total_units,
      ROUND(SUM(total_sales), 2) AS revenue,
      ROUND(AVG(total_sales), 2) AS avg_line_sales
    FROM `{source_stg_table}`
    GROUP BY order_date, Country
    """