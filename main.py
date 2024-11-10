from config import PDF_PATH, TOP_N_WORDS, SAVE_RESULTS_TO_CSV, CSV_OUTPUT_PATH
from text_extraction import extract_text_from_pdf
from word_count import get_word_counts, save_results_to_csv
from visualization import plot_top_words
from utils import get_spark_session
import time  # Importa el módulo time para medir tiempos de ejecución

def main():
    start_time = time.time()  # Inicia la medición del tiempo de ejecución

    # Extraer texto del PDF
    pdf_text = extract_text_from_pdf(PDF_PATH)

    # Procesar el texto solo si la extracción fue exitosa
    if pdf_text:
        spark = get_spark_session()  # Crear sesión de Spark para procesamiento
        word_counts_df = get_word_counts(pdf_text, TOP_N_WORDS, spark)

        # Mostrar resultados en consola
        word_counts_df.show()

        # Guardar resultados en CSV si está habilitado en la configuración
        if SAVE_RESULTS_TO_CSV:
            save_results_to_csv(word_counts_df, CSV_OUTPUT_PATH)

        # Generar gráfico de las palabras más frecuentes
        plot_top_words(word_counts_df)

        # Detener la sesión de Spark
        spark.stop()
    else:
        print("No se pudo extraer texto del PDF.")

    end_time = time.time()  # Fin de la medición de tiempo
    elapsed_time = end_time - start_time  # Calcula el tiempo total de ejecución
    print(f"Tiempo total de ejecución: {elapsed_time:.2f} segundos")  # Imprime el tiempo

if __name__ == "__main__":
    main()