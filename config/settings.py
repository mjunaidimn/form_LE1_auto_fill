"""
Application Configuration Settings
"""

from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = DATA_DIR / "templates"
SAMPLES_DIR = DATA_DIR / "samples"
OUTPUT_DIR = DATA_DIR / "output"

# Create directories if they don't exist
for directory in [DATA_DIR, TEMPLATES_DIR, SAMPLES_DIR, OUTPUT_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


class AppConfig:
    """Application configuration"""
    PAGE_TITLE = "Form E Auto-Fill"
    PAGE_ICON = "📝"
    LAYOUT = "wide"


class PDFConfig:
    """PDF processing configuration"""
    DPI = 200
    DEFAULT_ZOOM = DPI / 72


from src.models.field_definition import FieldType

class FieldConfig:
    """Field configuration"""
    DEFAULT_FONT_SIZE = 9
    DEFAULT_FONT_NAME = "Helvetica"
    FIELD_TYPES = [ft.value for ft in FieldType]


class ExportConfig:
    """Export configuration"""
    PDF_FILENAME_COL = "A1"
    ZIP_FILENAME = "filled_tax_forms.zip"