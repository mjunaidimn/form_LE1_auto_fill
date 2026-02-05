"""Template Loading Module"""

import pandas as pd
from typing import Optional
from src.models.form_template import FormTemplate, FieldDefinition
from src.models.field_definition import FieldType

class TemplateLoader:
    """Load templates from CSV"""
    
    @staticmethod
    def from_csv(csv_file) -> Optional[FormTemplate]:
        """Load template from CSV file"""
        try:
            df = pd.read_csv(csv_file)
            
            # Check for required columns
            required_columns = ['field_name', 'page_number', 'x', 'y']
            missing = [col for col in required_columns if col not in df.columns]
            
            if missing:
                raise ValueError(f"Missing columns: {', '.join(missing)}")
            
            fields = []
            for _, row in df.iterrows():
                field = FieldDefinition(
                    field_name=row['field_name'],
                    page_number=int(row['page_number']),
                    x=float(row['x']),
                    y=float(row['y']),
                    field_type=FieldType(row.get('field_type', 'text')),
                    font_size=int(row.get('font_size', 9)),
                    font_name=row.get('font_name', 'Helvetica'),
                    max_width=int(row['max_width']) if pd.notna(row.get('max_width')) else None
                )
                fields.append(field)
            
            return FormTemplate(fields=fields)
        except Exception as e:
            raise ValueError(f"Error loading template: {str(e)}")