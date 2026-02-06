"""Spreadsheet Processing Module"""

import pandas as pd
import re
from typing import Optional


class SpreadsheetProcessor:
    """Process spreadsheet files"""
    
    @staticmethod
    def load_file(file) -> Optional[pd.DataFrame]:
        """Load spreadsheet from file"""
        try:
            if file.name.endswith('.csv'):
                return pd.read_csv(file)
            else:
                return pd.read_excel(file)
        except Exception as e:
            raise ValueError(f"Error loading spreadsheet: {str(e)}")
    
    
    @staticmethod
    def split_string(text, separator=' ', max_len=50) -> list[str]:
        words = text.split(separator)
        parts = []
        current = ""

        for word in words:
            # If adding this word exceeds limit, push current and start new chunk
            if len(current) + len(word) + (1 if current else 0) > max_len:
                parts.append(current)
                current = word
            else:
                current = word if not current else current + " " + word

        # Append last part if non-empty
        if current:
            parts.append(current)

        return parts
    

    # Extract state from the 'Correspondence address' column using malaysia_states list
    @staticmethod
    def extract_state(address) -> Optional[str]:
        """Extract state from address"""

        malaysia_states = [
            "JOHOR",
            "KEDAH",
            "KELANTAN",
            "MELAKA",
            "NEGERI SEMBILAN",
            "PAHANG",
            "PULAU PINANG",
            "PERAK",
            "PERLIS",
            "SABAH",
            "SARAWAK",
            "SELANGOR",
            "TERENGGANU",
            "WILAYAH PERSEKUTUAN KUALA LUMPUR",
            "WILAYAH PERSEKUTUAN PUTRAJAYA",
            "WILAYAH PERSEKUTUAN LABUAN",
            "FEDERAL TERRITORY OF LABUAN"
        ]

        for state in malaysia_states:
            if state in address:
                return state
        return None
    
    @staticmethod
    # Extract city by removing address_line, postcode, and state from the full address
    def extract_city(row, full_address: str, address_line: str, postcode: str, state: str) -> Optional[str]:
        address = row[full_address]
        address_line = row[address_line]
        postcode = row[postcode]
        state = row[state]
        
        return address.replace(address_line, '').replace(postcode, '').replace(state, '').strip()
    

    @staticmethod
    def process_file(df: pd.DataFrame) -> pd.DataFrame:
        """Process spreadsheet data"""

        # Remove leading/trailing whitespace from all column names
        df.columns = df.columns.str.strip()

        ######################################
        # PART A: BASIC PARTICULARS
        ######################################

        df['year_1'] = df['Tahun Taksiran'].astype(str)[0][0]
        df['year_2'] = df['Tahun Taksiran'].astype(str)[0][1]
        df['year_3'] = df['Tahun Taksiran'].astype(str)[0][2]
        df['year_4'] = df['Tahun Taksiran'].astype(str)[0][3]

        # Split employer name if exceeds 50 characters
        df.loc[:, 'employer_name_split'] = df.loc[:, 'A1'].str.upper().apply(lambda x: SpreadsheetProcessor.split_string(x, max_len=52))

        # Create new columns for split employer names
        df.loc[:, 'A1_1'] = df['employer_name_split'].apply(lambda x: x[0])
        df.loc[:, 'A1_2'] = df['employer_name_split'].apply(lambda x: x[1] if len(x) > 1 else '')

        # Extract address line before the 5-digit postcode
        df['A2_address_line'] = df['A2'].str.extract(r'^(.*?)(?=\b\d{5}\b)', expand=False).str.strip()

        # Split address if exceeds 62 characters
        df.loc[:, 'A2_address_line_split'] = df.loc[:, 'A2_address_line'].str.upper().apply(lambda x: SpreadsheetProcessor.split_string(x, separator=',', max_len=62))

        # Create new columns for split addresses
        df.loc[:, 'A2_address_1'] = df['A2_address_line_split'].apply(lambda x: x[0].strip().replace('  ', ', '))
        df.loc[:, 'A2_address_2'] = df['A2_address_line_split'].apply(lambda x: x[1].strip().replace('  ', ', ') if len(x) > 1 else '')
        
        # Extract 5-digit postcode
        df['A2_postcode'] = df['A2'].str.extract(r'(\b\d{5}\b)', expand=False)

        # Extract state from the 'Correspondence address' column
        df['A2_state'] = df['A2'].apply(SpreadsheetProcessor.extract_state)

        # Extract city from the address column
        df['A2_city'] = df.apply(lambda row: SpreadsheetProcessor.extract_city(row, 'A2', 'A2_address_line', 'A2_postcode', 'A2_state'), axis=1)

        # After extracting city, shorten state names
        df['A2_state'] = df['A2_state'].replace({
            "WILAYAH PERSEKUTUAN": "W.P.",
            "FEDERAL TERRITORY OF": "F.T."
        }, regex=True)

        # Extract only digits from the TIN in the "A3" column
        df.loc[:, 'A3'] = df.loc[:, 'A3'].str.extract('(\d+)', expand=False)

        # Convert to datetime
        df['A7_datetime'] = pd.to_datetime(df['A7 (Commencement Date / Incorporation Date)'], format='mixed')

        # Extract day, month, year padded with leading zeros and spaced out
        df['A7_day'] = df['A7_datetime'].dt.day.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A7_month'] = df['A7_datetime'].dt.month.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A7_year'] = df['A7_datetime'].dt.year.apply(lambda x: '   '.join(list(str(x).zfill(2))))

        # Split A10 into 'from' and 'to' dates
        df[['A10_from', 'A10_to']] = df['A10'].str.split(' hingga ', n=1, expand=True)
        df[['A11_from', 'A11_to']] = df['A11'].str.split(' hingga ', n=1, expand=True)

        # Convert A10_from and A10_to to datetime
        df['A10_from_datetime'] = pd.to_datetime(df['A10_from'], format='%d-%m-%Y')
        df['A10_to_datetime'] = pd.to_datetime(df['A10_to'], format='%d-%m-%Y')

        # Convert A11_from and A11_to to datetime
        df['A11_from_datetime'] = pd.to_datetime(df['A11_from'], format='%d-%m-%Y')
        df['A11_to_datetime'] = pd.to_datetime(df['A11_to'], format='%d-%m-%Y')

        # Extract day, month, year for A10_from
        df['A10_from_day'] = df['A10_from_datetime'].dt.day.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A10_from_month'] = df['A10_from_datetime'].dt.month.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A10_from_year'] = df['A10_from_datetime'].dt.year.apply(lambda x: '   '.join(list(str(x).zfill(4))))

        # Extract day, month, year for A10_to
        df['A10_to_day'] = df['A10_to_datetime'].dt.day.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A10_to_month'] = df['A10_to_datetime'].dt.month.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A10_to_year'] = df['A10_to_datetime'].dt.year.apply(lambda x: '   '.join(list(str(x).zfill(4))))

        # Extract day, month, year for A11_from
        df['A11_from_day'] = df['A11_from_datetime'].dt.day.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A11_from_month'] = df['A11_from_datetime'].dt.month.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A11_from_year'] = df['A11_from_datetime'].dt.year.apply(lambda x: '   '.join(list(str(x).zfill(4))))

        # Extract day, month, year for A11_to
        df['A11_to_day'] = df['A11_to_datetime'].dt.day.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A11_to_month'] = df['A11_to_datetime'].dt.month.apply(lambda x: '  '.join(list(str(x).zfill(2))))
        df['A11_to_year'] = df['A11_to_datetime'].dt.year.apply(lambda x: '   '.join(list(str(x).zfill(4))))

        # Ensure 4 decimal places for A14
        df['A14'] = df['A14'].apply(lambda x: '{:.4f}'.format(x) if pd.notnull(x) else x)


        ######################################
        # PART C: PARTICULARS OF LABUAN ENTITY
        ######################################

        # Extract address line before the 5-digit postcode
        df['C1_address_line'] = df['C1'].str.extract(r'^(.*?)(?=\b\d{5}\b)', expand=False).str.strip()

        # Split address if exceeds 62 characters
        df.loc[:, 'C1_address_line_split'] = df.loc[:, 'C1_address_line'].str.upper().apply(lambda x: SpreadsheetProcessor.split_string(x, separator=',', max_len=62))

        # Create new columns for split addresses
        df.loc[:, 'C1_address_1'] = df['C1_address_line_split'].apply(lambda x: x[0].strip().replace('  ', ', '))
        df.loc[:, 'C1_address_2'] = df['C1_address_line_split'].apply(lambda x: x[1].strip().replace('  ', ', ') if len(x) > 1 else '')
        
        # Extract 5-digit postcode
        df['C1_postcode'] = df['C1'].str.extract(r'(\b\d{5}\b)', expand=False)

        # Extract state from the address column
        df['C1_state'] = df['C1'].apply(SpreadsheetProcessor.extract_state)

         # Extract city from the address column
        df['C1_city'] = df.apply(lambda row: SpreadsheetProcessor.extract_city(row, 'C1', 'C1_address_line', 'C1_postcode', 'C1_state'), axis=1)

        # After extracting city, shorten state names
        df['C1_state'] = df['C1_state'].replace({
            "WILAYAH PERSEKUTUAN": "W.P.",
            "FEDERAL TERRITORY OF": "F.T."
        }, regex=True)

        df['C1_country'] = 'MALAYSIA'

        # Rename (remove newline characters)
        df.columns = df.columns.str.replace('\n', ' ')

        # Subset relevant columns only
        df = df[[
            'year_1', 'year_2', 'year_3', 'year_4',
            'A1_1', 'A1_2',
            'A2_address_1', 'A2_address_2', 'A2_postcode', 'A2_city', 'A2_state',
            'A3', 'A4', 'A5', 'A6',
            'A7_day', 'A7_month', 'A7_year',
            'A8', 'A9',
            'A10_from_day', 'A10_from_month', 'A10_from_year',
            'A10_to_day', 'A10_to_month', 'A10_to_year',
            'A11_from_day', 'A11_from_month', 'A11_from_year',
            'A11_to_day', 'A11_to_month', 'A11_to_year',
            'A12', 'A13', 'A14', 'A15', 'A16', 'A17', 'A18', 'A19', 'A20',
            'C1_address_1', 'C1_address_2', 'C1_postcode', 'C1_city', 'C1_state', 'C1_country',
            'C2',
            'C6a',
            'C6b = B5',
            'C7a', 'C7b',
            'C8a', 'C8b',
            'C10', 'C11', 'C12',
            'D1', 'D2', 'D3', 'D4'
        ]]

        return df