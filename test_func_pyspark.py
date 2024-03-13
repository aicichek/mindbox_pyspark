from func_pyspark import get_product_category_pairs
from pyspark.sql import SparkSession

# Создаем SparkSession
spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

# Создаем тестовые данные
products = [
    ("p1", "Product 1"),
    ("p2", "Product 2"),
    ("p3", "Product 3"),
    ("p4", "Product 4"),
    ("p5", "Product 5")
]
products_df = spark.createDataFrame(products, ["product_id", "product_name"])

categories = [
    ("c1", "Category 1"),
    ("c2", "Category 2"),
    ("c3", "Category 3")
]
categories_df = spark.createDataFrame(categories, ["category_id", "category_name"])

product_category = [
    ("p1", "c1"),
    ("p1", "c2"),
    ("p2", "c2"),
    ("p3", "c3"),
    ("p5", "c1")
]
product_category_df = spark.createDataFrame(product_category, ["product_id", "category_id"])

# Вызываем функцию с тестовыми данными
result = get_product_category_pairs(products_df, categories_df, product_category_df)

# Выводим результат
result.show(truncate=False)