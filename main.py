# main.py

from config import PDF_PATH, TOP_N_WORDS, SAVE_RESULTS_TO_CSV, CSV_OUTPUT_PATH
from text_extraction import extract_text_from_pdf
from word_count import get_word_counts, save_results_to_csv
from visualization import plot_top_words
from utils import get_spark_session

def main():
    # Extraer texto del PDF
    pdf_text = extract_text_from_pdf(PDF_PATH)

    # Contar palabras en el texto extraído
    if pdf_text:
        spark = get_spark_session()
        word_counts_df = get_word_counts(pdf_text, TOP_N_WORDS)

        # Mostrar resultados
        word_counts_df.show()

        # Guardar resultados en CSV si está habilitado
        if SAVE_RESULTS_TO_CSV:
            save_results_to_csv(word_counts_df, CSV_OUTPUT_PATH)

        # Graficar resultados
        plot_top_words(word_counts_df)

        # Finalizar Spark
        spark.stop()
    else:
        print("No se pudo extraer texto del PDF.")

if __name__ == "__main__":
    main()