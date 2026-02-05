"""PDF Processing Module"""

from typing import List
from PIL import Image
from io import BytesIO

try:
    import fitz
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False


class PDFProcessor:
    """PDF to image conversion"""
    
    def __init__(self, dpi: int = 200):
        self.dpi = dpi
        self.zoom = dpi / 72
        
        if not PYMUPDF_AVAILABLE:
            raise ImportError("PyMuPDF required. Install: pip install PyMuPDF")
    
    def pdf_to_images(self, pdf_bytes: bytes) -> List[Image.Image]:
        """Convert PDF to list of PIL Images"""
        try:
            images = []
            pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
            mat = fitz.Matrix(self.zoom, self.zoom)
            
            for page_num in range(pdf_document.page_count):
                page = pdf_document[page_num]
                pix = page.get_pixmap(matrix=mat)
                img_data = pix.tobytes("ppm")
                img = Image.frombytes("RGB", [pix.width, pix.height], img_data)
                images.append(img)
            
            pdf_document.close()
            return images
        except Exception as e:
            raise ValueError(f"Failed to convert PDF: {str(e)}")