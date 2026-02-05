"""Coordinate Conversion Utilities"""


class CoordinateUtils:
    """Convert between image and PDF coordinates"""
    
    @staticmethod
    def image_to_pdf(x: float,
                     y: float,
                     image_height: float,
                     pdf_height: float = 792) -> tuple:
        """
        Convert image coords to PDF coords
        PDF origin: bottom-left
        Image origin: top-left
        """
        scale = pdf_height / image_height
        pdf_x = x * scale
        pdf_y = pdf_height - (y * scale)
        return pdf_x, pdf_y