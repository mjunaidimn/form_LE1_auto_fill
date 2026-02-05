"""Form Template Data Model"""

from dataclasses import dataclass, field
from typing import List, Dict, Any
from .field_definition import FieldDefinition


@dataclass
class FormTemplate:
    """Complete form template"""
    fields: List[FieldDefinition]
    template_name: str = "template"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_fields_by_page(self, page_number: int) -> List[FieldDefinition]:
        """Get all fields for a specific page"""
        return [f for f in self.fields if f.page_number == page_number]
    
    def get_field_names(self) -> List[str]:
        """Get list of all field names"""
        return [f.field_name for f in self.fields]
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        return {
            "template_name": self.template_name,
            "fields": [f.to_dict() for f in self.fields],
            "metadata": self.metadata
        }