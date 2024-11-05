# text_extraction.py

from PyPDF2 import PdfReader

def extract_text_from_pdf(pdf_path):
    """Extrae texto del archivo PDF dado."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        return text
    except Exception as e:
        print(f"Error al leer el archivo PDF: {e}")
        return ""