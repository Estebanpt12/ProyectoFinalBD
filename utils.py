from pyspark.sql import SparkSession

def get_spark_session(app_name="WordFrequencyAnalysis"):
    """Crea y devuelve una sesión de Spark."""
    return SparkSession.builder.appName(app_name).getOrCreate()
