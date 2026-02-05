"""Field Definition Data Model"""

from dataclasses import dataclass, asdict
from typing import Optional
from enum import Enum


class FieldType(Enum):
    """Types of form fields"""
    TEXT = "text"
    NUMBER = "number"
    DATE = "date"
    CHECKBOX = "checkbox"


@dataclass
class FieldDefinition:
    """Represents a field in a form template"""
    field_name: str
    page_number: int
    x: float
    y: float
    field_type: FieldType = FieldType.TEXT
    font_size: int = 10
    font_name: str = "Helvetica"
    max_width: Optional[int] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary"""
        data = asdict(self)
        data['field_type'] = self.field_type.value
        return data
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FieldDefinition':
        """Create from dictionary"""
        return cls(
            field_name=data['field_name'],
            page_number=int(data['page_number']),
            x=float(data['x']),
            y=float(data['y']),
            field_type=FieldType(data.get('field_type', 'text')),
            font_size=int(data.get('font_size', 10)),
            font_name=data.get('font_name', 'Helvetica'),
            max_width=int(data['max_width']) if data.get('max_width') else None
        )