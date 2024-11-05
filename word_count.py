# word_count.py

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col

# Configurar sesión de Spark
spark = SparkSession.builder.appName("WordFrequencyPDF").getOrCreate()

# Lista de palabras comunes en inglés que queremos filtrar
STOPWORDS = {"the", "and", "of", "to", "a", "in", "that", "it", "is", "was", "he", "for", "on", "are", "with", "as", "his", "they", "at"}

def clean_and_split_text(text):
    """Limpia y separa el texto en palabras."""
    words = re.findall(r'\b\w+\b', text.lower())
    return [word for word in words if word not in STOPWORDS]

def get_word_counts(text, top_n):
    """Cuenta la frecuencia de palabras y devuelve las más comunes."""
    words_rdd = spark.sparkContext.parallelize([text]).flatMap(clean_and_split_text)
    words_df = words_rdd.toDF(["word"])
    word_counts = words_df.groupBy("word").count().orderBy(desc("count"))
    return word_counts.limit(top_n)

def save_results_to_csv(df, output_path):
    """Guarda el DataFrame con los conteos de palabras en un archivo CSV."""
    df.toPandas().to_csv(output_path, index=False)