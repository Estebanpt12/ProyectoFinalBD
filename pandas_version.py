import pandas as pd
import re
import nltk
import time
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
from nltk.stem import WordNetLemmatizer
from PyPDF2 import PdfReader
import matplotlib.pyplot as plt

# Descargar los paquetes necesarios de nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Variables de configuración (asegúrate de que el archivo PDF esté en la misma carpeta o proporciona la ruta completa)
PDF_PATH = "Document.pdf"
TOP_N_WORDS = 20
SAVE_RESULTS_TO_CSV = True
CSV_OUTPUT_PATH = "top_words_pandas.csv"

# Extraer texto del PDF
def extract_text_from_pdf(pdf_path):
    """Extrae texto de un archivo PDF."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
        return text
    except Exception as e:
        print(f"Error al leer el archivo PDF: {e}")
        return ""  # Retorna una cadena vacía en caso de error

# Preprocesar texto y contar palabras
def get_word_counts_pandas(text, top_n):
    """Limpia el texto, tokeniza, lematiza, cuenta palabras y obtiene las 'top_n' más frecuentes usando Pandas."""
    # Limpiar caracteres especiales y convertir a minúsculas
    cleaned_text = re.sub('[^A-Za-z0-9 ]+', ' ', text).lower()

    # Tokenizar el texto en palabras
    tokens = word_tokenize(cleaned_text)

    # Eliminar stopwords
    stopwords = set(nltk.corpus.stopwords.words('english'))
    lemmatizer = WordNetLemmatizer()
    tokens_filtered = [lemmatizer.lemmatize(word) for word in tokens if word not in stopwords]

    # Contar la frecuencia de cada palabra
    fdist = FreqDist(tokens_filtered)

    # Obtener las 'top_n' palabras más frecuentes
    top_words = fdist.most_common(top_n)

    # Convertir a DataFrame para facilidad de uso y exportación
    word_counts_df = pd.DataFrame(top_words, columns=['word', 'count'])

    return word_counts_df

# Guardar resultados en un archivo CSV
def save_results_to_csv_pandas(df, output_path):
    """Guarda el DataFrame en un archivo CSV."""
    df.to_csv(output_path, index=False)

# Graficar las palabras más frecuentes
def plot_top_words_pandas(df):
    """Genera un gráfico de barras con las palabras más frecuentes."""
    plt.figure(figsize=(10, 6))
    plt.barh(df['word'], df['count'], color="skyblue")
    plt.xlabel("Frecuencia")
    plt.ylabel("Palabra")
    plt.title("Top Palabras Más Frecuentes (Pandas)")
    plt.gca().invert_yaxis()
    plt.show()

# Función principal
def main_pandas():
    start_time = time.time()

    # Extraer texto del PDF
    pdf_text = extract_text_from_pdf(PDF_PATH)

    # Procesar el texto solo si la extracción fue exitosa
    if pdf_text:
        # Contar palabras y obtener las 'top_n' más frecuentes
        word_counts_df = get_word_counts_pandas(pdf_text, TOP_N_WORDS)

        # Mostrar los resultados en la consola
        print(word_counts_df)

        # Guardar los resultados en un archivo CSV
        if SAVE_RESULTS_TO_CSV:
            save_results_to_csv_pandas(word_counts_df, CSV_OUTPUT_PATH)

        # Generar gráfico de las palabras más frecuentes
        plot_top_words_pandas(word_counts_df)
    else:
        print("No se pudo extraer texto del PDF.")

    # Calcular y mostrar el tiempo de ejecución
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo total de ejecución: {elapsed_time:.2f} segundos")

# Ejecutar la función principal
if __name__ == "__main__":
    main_pandas()
