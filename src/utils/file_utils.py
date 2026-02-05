"""File Utilities"""

from io import BytesIO
from typing import List
import zipfile


class FileUtils:
    """File operation utilities"""
    
    @staticmethod
    def create_zip(pdf_files: List[BytesIO], filenames: List[str]) -> BytesIO:
        """Create zip file containing PDFs"""
        zip_buffer = BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for pdf_data, filename in zip(pdf_files, filenames):
                zip_file.writestr(filename, pdf_data.getvalue())
        
        zip_buffer.seek(0)
        return zip_buffer