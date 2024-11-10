from PyPDF2 import PdfReader

def extract_text_from_pdf(pdf_path):
    """Extrae texto de un archivo PDF, optimizado para archivos grandes."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            # Extrae texto de cada página y evita agregar valores None
            text += page.extract_text() or ""
        return text
    except Exception as e:
        print(f"Error al leer el archivo PDF: {e}")
        return ""  # Retorna una cadena vacía en caso de error