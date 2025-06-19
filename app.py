import pandas as pd
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename
import io

app = Flask(__name__)

@app.route('/api/analyze', methods=['POST'])
def analyze():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    try:
        # Read CSV (semicolon-delimited, handle EAN quotes)
        df = pd.read_csv(file, sep=';', quotechar='"', skipinitialspace=True)

        # Check required columns
        required_columns = [
            'Van (datum)', 'Van (tijdstip)', 'Register', 'Volume'
        ]
        if not all(col in df.columns for col in required_columns):
            return jsonify({'error': 'Missing columns in CSV'}), 422

        # Parse start datetime
        df['datetime'] = pd.to_datetime(df['Van (datum)'] + ' ' + df['Van (tijdstip)'], dayfirst=True)

        # Add 'month' column
        df['month'] = df['datetime'].dt.to_period('M').astype(str)

        # Normalize Register field
        df['register_clean'] = df['Register'].str.lower().str.strip()

        # Group by month and register: sum Volume
        group = df.groupby(['month', 'register_clean'])['Volume'].sum().reset_index()

        # Highest quarter-hourly value for afname (day or night)
        afname_mask = df['register_clean'].str.startswith('afname')
        max_afname = df.loc[afname_mask, 'Volume'].max()

        result = {
            "monthly_totals": group.to_dict(orient='records'),
            "max_afname_quarter": max_afname
        }

        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': f'Could not process CSV: {str(e)}'}), 400

@app.route('/')
def index():
    return "Energy backend is running!"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
