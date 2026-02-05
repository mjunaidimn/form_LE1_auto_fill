"""Text Processing Utilities"""

import pandas as pd
from typing import Any


class TextUtils:
    """Text formatting utilities"""
    
    @staticmethod
    def format_value(value: Any, field_type: str) -> str:
        """Format value based on field type"""
        if pd.isna(value):
            return ""
        
        if field_type == "number":
            try:
                return f"{float(value):,.0f}"
            except (ValueError, TypeError):
                return str(value)
        elif field_type == "date":
            if isinstance(value, pd.Timestamp):
                return value.strftime("%d-%m-%Y")
            return str(value)
        elif field_type == "checkbox":
            return "✓" if value else ""
        else:
            # If pandas read a number as float (e.g., 123.0), convert to int then str
            if isinstance(value, float) and value.is_integer():
                return str(int(value))
            return str(value)
    
    @staticmethod
    def truncate_to_width(text: str, max_width: int, font_size: int) -> str:
        """Truncate text to fit width"""
        if max_width is None:
            return text
        
        avg_char_width = font_size * 0.6
        max_chars = int(max_width / avg_char_width)
        
        if len(text) <= max_chars:
            return text
        
        return text[:max_chars-3] + "..."