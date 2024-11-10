import re
import nltk
from pyspark.sql.functions import desc
from pyspark.sql import Row
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

# Descargar stopwords de NLTK si es necesario
# nltk.download('stopwords')
# nltk.download('wordnet')

# Configura las stopwords y el lematizador
STOPWORDS = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def clean_and_split_text(text):
    """Limpia y separa el texto en palabras, eliminando stopwords y aplicando lematización."""
    # Elimina caracteres no alfanuméricos y convierte a minúsculas
    text = re.sub(r'[^a-zA-Z\s]', '', text.lower())
    # Divide en palabras
    words = re.findall(r'\b\w+\b', text)
    # Elimina stopwords y lematiza las palabras
    cleaned_words = [lemmatizer.lemmatize(word) for word in words if word not in STOPWORDS]
    return cleaned_words

def get_word_counts(text, top_n, spark):
    """Cuenta la frecuencia de palabras en el texto y devuelve las 'top_n' más comunes."""
    # Limpia y divide el texto en palabras
    words = clean_and_split_text(text)
    # Crea un RDD con cada palabra
    words_rdd = spark.sparkContext.parallelize(words).map(lambda word: Row(word=word))
    # Convierte el RDD a un DataFrame de Spark
    words_df = spark.createDataFrame(words_rdd)
    # Cuenta palabras y ordena por frecuencia
    word_counts = words_df.groupBy("word").count().orderBy(desc("count"))
    return word_counts.limit(top_n)

def save_results_to_csv(df, output_path):
    """Guarda el DataFrame con los conteos de palabras en un archivo CSV."""
    df.toPandas().to_csv(output_path, index=False)  # Convierte a Pandas y guarda como CSV
