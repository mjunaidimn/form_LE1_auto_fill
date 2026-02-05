"""
PDF Form Filler Application
Batch-fill PDF forms using CSV templates
"""

import streamlit as st
import pandas as pd
from io import BytesIO
from typing import Dict, List
from PIL import Image

# Import modules
from config.settings import AppConfig, PDFConfig, ExportConfig
from src.models.form_template import FormTemplate
from src.core.pdf_processor import PDFProcessor
from src.core.coordinate_utils import CoordinateUtils
from src.io.template_loader import TemplateLoader
from src.io.spreadsheet_processor import SpreadsheetProcessor
from src.utils.text_utils import TextUtils
from src.utils.file_utils import FileUtils

# ReportLab imports
try:
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.utils import ImageReader
    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


# ============================================================================
# PDF GENERATION
# ============================================================================

class PDFGenerator:
    """Generate filled PDFs"""
    
    @staticmethod
    def create_filled_pdf(pdf_images: List[Image.Image],
                         template: FormTemplate,
                         field_data: Dict[str, any]) -> BytesIO:
        """Create PDF with overlaid data"""
        if not REPORTLAB_AVAILABLE:
            raise ImportError("ReportLab required")
        
        pdf_buffer = BytesIO()
        c = canvas.Canvas(pdf_buffer, pagesize=letter)
        page_width, page_height = letter
        
        for page_idx, page_img in enumerate(pdf_images):
            # Draw background image
            img_reader = ImageReader(page_img)
            img_width, img_height = page_img.size
            
            scale = min(page_width / img_width, page_height / img_height)
            scaled_width = img_width * scale
            scaled_height = img_height * scale
            
            x_offset = (page_width - scaled_width) / 2
            y_offset = (page_height - scaled_height) / 2
            
            c.drawImage(img_reader, x_offset, y_offset,
                       width=scaled_width, height=scaled_height)
            
            # Overlay field data
            page_fields = template.get_fields_by_page(page_idx)
            
            for field in page_fields:
                if field.field_name in field_data:
                    value = field_data[field.field_name]
                    formatted = TextUtils.format_value(value, field.field_type.value)
                    
                    if formatted:
                        if field.max_width:
                            formatted = TextUtils.truncate_to_width(
                                formatted, field.max_width, field.font_size
                            )
                        
                        pdf_x, pdf_y = CoordinateUtils.image_to_pdf(
                            field.x, field.y, img_height, page_height
                        )
                        
                        # pdf_x = pdf_x * scale + x_offset
                        # pdf_y = pdf_y * scale + y_offset
                        pdf_x = pdf_x + x_offset
                        pdf_y = pdf_y + y_offset
                        
                        c.setFont(field.font_name, field.font_size)
                        c.setFillColorRGB(0, 0, 0)
                        c.drawString(pdf_x, pdf_y, formatted)
            
            c.showPage()
        
        c.save()
        pdf_buffer.seek(0)
        return pdf_buffer


# ============================================================================
# SESSION STATE
# ============================================================================

def init_session_state():
    """Initialize session state"""
    defaults = {
        'template': None,
        'pdf_images': None,
        'spreadsheet_data': None,
        'column_field_mapping': {}
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# ============================================================================
# UI COMPONENTS
# ============================================================================

def render_header():
    """Render header"""
    st.title("📝 Form E Auto-Fill")
    st.markdown("""
    Batch-fill Form E using CSV templates.
    Upload your template, PDF, and data to generate filled forms.
    """)


def render_upload_section():
    """Render upload section"""
    st.header("Step 1: Upload Files")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("📄 PDF Form")
        pdf_file = st.file_uploader("Upload PDF", type=['pdf'], key='pdf')
        
        if pdf_file:
            try:
                processor = PDFProcessor(dpi=PDFConfig.DPI)
                images = processor.pdf_to_images(pdf_file.read())
                st.session_state.pdf_images = images
                st.success(f"✓ Loaded {len(images)} pages")
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    with col2:
        st.subheader("📊 Data Spreadsheet")
        data_file = st.file_uploader("Upload data", type=['xlsx', 'xls', 'csv'], key='data')
        
        if data_file:
            try:
                df = SpreadsheetProcessor.load_file(data_file)
                df = SpreadsheetProcessor.process_file(df)
                st.session_state.spreadsheet_data = df
                st.success(f"✓ Loaded {len(df)} rows")
                
                with st.expander("Preview"):
                    st.dataframe(df.head())
            except Exception as e:
                st.error(f"Error: {str(e)}")
    
    with col3:
        st.subheader("📋 Template CSV")
        template_file = st.file_uploader("Upload template", type=['csv'], key='template_uploader')
        
        if template_file:
            try:
                template = TemplateLoader.from_csv(template_file)
                st.session_state.template = template
                st.success(f"✓ Loaded {len(template.fields)} fields")
                
                with st.expander("View Fields"):
                    for f in template.fields:
                        st.text(f"• {f.field_name} (Page {f.page_number + 1})")
            except Exception as e:
                st.error(f"Error: {str(e)}")


def render_mapping_section():
    """Render mapping section"""
    template = st.session_state.get('template')
    df = st.session_state.get('spreadsheet_data')
    
    if not template or df is None:
        return
    
    st.header("Step 2: Map Columns to Fields")
    
    if st.button("🔮 Auto-Map Fields"):
        field_names = template.get_field_names()
        auto_map = {col: col for col in df.columns if col in field_names}
        st.session_state.column_field_mapping = auto_map
        
        # Update widget states to reflect auto-mapping
        for col, field in auto_map.items():
            st.session_state[f"sel_{col}"] = field
            
        st.success(f"Auto-mapped {len(auto_map)} fields")
    
    st.divider()
    
    field_names = template.get_field_names()
    mapping = st.session_state.column_field_mapping
    new_mapping = {}
    
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.markdown("**Column**")
    with col2:
        st.markdown("**Field**")
    with col3:
        st.markdown("**Status**")
    
    for column in df.columns:
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            st.text(column)
        
        with col2:
            current = mapping.get(column, "")
            options = [""] + field_names
            
            try:
                idx = options.index(current) if current in field_names else 0
            except ValueError:
                idx = 0
            
            selected = st.selectbox(
                f"map_{column}",
                options=options,
                index=idx,
                key=f"sel_{column}",
                label_visibility="collapsed"
            )
            
            if selected:
                new_mapping[column] = selected
        
        with col3:
            if column in new_mapping:
                st.success("✓")
    
    st.session_state.column_field_mapping = new_mapping
    
    if new_mapping:
        st.info(f"**{len(new_mapping)}** columns mapped")


def render_generation_section():
    """Render generation section"""
    template = st.session_state.get('template')
    images = st.session_state.get('pdf_images')
    df = st.session_state.get('spreadsheet_data')
    mapping = st.session_state.get('column_field_mapping')
    
    if not all([template, images, df is not None, mapping]):
        return
    
    st.header("Step 3: Generate PDFs")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Forms", len(df))
    with col2:
        st.metric("Fields", len(mapping))
    with col3:
        st.metric("Pages", len(images))
    
    if st.button("🚀 Generate All PDFs", type="primary", use_container_width=True):
        generate_pdfs(template, images, df, mapping)


def generate_pdfs(template, images, df, mapping):
    """Generate all PDFs"""
    with st.spinner("Generating PDFs..."):
        progress = st.progress(0)
        pdf_files = []
        filenames = []
        
        for idx, row in df.iterrows():
            field_data = {}
            for col, field_name in mapping.items():
                if pd.notna(row[col]):
                    field_data[field_name] = row[col]
            
            pdf = PDFGenerator.create_filled_pdf(images, template, field_data)
            
            name = df[ExportConfig.PDF_FILENAME_COL].iloc[idx]
            filename = f"{str(name)}.pdf"
            
            pdf_files.append(pdf)
            filenames.append(filename)
            progress.progress((idx + 1) / len(df))
        
        st.success(f"✅ Generated {len(pdf_files)} PDFs!")
        
        col1, col2 = st.columns(2)
        
        with col1:
            zip_file = FileUtils.create_zip(pdf_files, filenames)
            st.download_button(
                "📦 Download All as ZIP",
                zip_file,
                ExportConfig.ZIP_FILENAME,
                "application/zip",
                use_container_width=True
            )
        
        with col2:
            st.download_button(
                "📄 Download First PDF",
                pdf_files[0],
                filenames[0],
                "application/pdf",
                use_container_width=True
            )


# ============================================================================
# MAIN APP
# ============================================================================

def main():
    """Main application"""
    st.set_page_config(
        page_title=AppConfig.PAGE_TITLE,
        page_icon=AppConfig.PAGE_ICON,
        layout=AppConfig.LAYOUT
    )
    
    init_session_state()
    
    render_header()
    render_upload_section()
    
    st.divider()
    render_mapping_section()
    
    st.divider()
    render_generation_section()
    
    st.markdown("---")
    st.markdown("💡 Upload template CSV (with field coordinates), PDF, and data to batch-fill forms")


if __name__ == "__main__":
    main()