from pyspark.sql.functions import col, collect_list, array_contains, lit, when

def get_product_category_pairs(products_df, categories_df, product_category_df):
    """
    Метод, который возвращает DataFrame со всеми парами "Имя продукта - Имя категории"
    и именами всех продуктов без категорий.
    """
    # Объединяем продукты и связи продуктов с категориями
    product_category_joined = products_df.join(product_category_df, on="product_id", how="left")
    
    # Группируем по продукту и собираем все связанные категории в список
    product_categories = product_category_joined.groupBy("product_name") \
                                                .agg(collect_list("category_id").alias("category_ids"))
    
    # Объединяем список категорий с названиями категорий
    product_category_pairs = product_categories.join(categories_df, array_contains(product_categories.category_ids, categories_df.category_id), how="outer") \
                                               .select("product_name", when(col("category_name").isNull(), lit(None)).otherwise(col("category_name")).alias("category_name"))
    
    # Сортируем по названию продукта
    product_category_pairs = product_category_pairs.orderBy("product_name")

    return product_category_pairs