# PDF Form Filler

Batch-fill PDF forms using CSV templates.

## Installation
```bash
pip install -r requirements.txt
```

## Usage
```bash
streamlit run app.py
```

## Workflow

1. Create template CSV with field coordinates
2. Upload template, PDF, and data spreadsheet
3. Map columns to fields
4. Generate filled PDFs

## Template CSV Format
```csv
field_name,page_number,x,y,field_type,font_size
Full Name,0,120,250,text,10
Amount,0,450,300,number,10
```