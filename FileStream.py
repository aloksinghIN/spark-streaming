from pyspark.sql import *
from pyspark.sql.functions import expr

if __name__ == '__main__':
    spark = SparkSession.builder.appName("file stream") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    raw_df = spark.readStream.format("json").option("path","data/*.json")\
        .option("maxFilesPerTrigger",1).load()
    explode_df = explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                                   "CustomerType", "PaymentMethod", "DeliveryType", "DeliveryAddress.City",
                                   "DeliveryAddress.State",
                                   "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    invoiceWriterQuery = flattened_df.writeStream \
        .format("json") \
        .queryName("Flattened Invoice Writer") \
        .outputMode("append") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    print("Flattened Invoice Writer started")
    invoiceWriterQuery.awaitTermination()