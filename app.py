import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta
import calendar
import csv
import io

app = Flask(__name__)
CORS(app)

# --- Load static data once ---
indexes_df = pd.read_excel("data/indexes.xlsx")
dnb_postalcode_df = pd.read_excel("data/dnb_postalcode.xlsx")
supplier_product_df = pd.read_excel("data/supplier_product.xlsx")
supplier_product_tariffs_df = pd.read_excel("data/supplier_product_tariffs.xlsx")
grid_and_levies_df = pd.read_excel("data/grid_and_levies.xlsx")

EXPECTED_COLUMNS = [
    "Van (datum)", "Van (tijdstip)", "Tot (datum)", "Tot (tijdstip)",
    "EAN-code", "Meter", "Metertype", "Register", "Volume",
    "Eenheid", "Validatiestatus", "Omschrijving"
]

print("hello world!")


def is_csv_corrupted(df):
    if df.shape[1] > len(EXPECTED_COLUMNS): return True
    if df.columns[0].count(';') >= 5: return True
    if 'Volume' in df.columns and df['Volume'].dropna().astype(str).str.fullmatch(r'\d+').all(): return True
    return False

def smart_parse_corrupted_energy_csv(file_path):
    df = pd.read_csv(file_path, delimiter=';', encoding='utf-8')
    if is_csv_corrupted(df):
        raise ValueError("CSV appears to be corrupted. Try uploading the original export.")
    return df

def load_user_meter_data(csv_path):
    df = smart_parse_corrupted_energy_csv(csv_path)
    df['Volume'] = df['Volume'].str.replace(',', '.', regex=False).astype(float)
    df['datetime'] = pd.to_datetime(df['Van (datum)'] + ' ' + df['Van (tijdstip)'], dayfirst=True)
    df['month'] = df['datetime'].dt.to_period('M').astype(str)
    df['register_clean'] = df['Register'].str.lower().str.strip()
    return df

def resolve_supplier_backend_name(supplier, product):
    match = supplier_product_df[
        (supplier_product_df['supplier_frontend'].str.lower() == supplier.lower()) &
        (supplier_product_df['product_name_frontend'].str.lower() == product.lower())
    ]
    if match.empty:
        raise ValueError("Unknown supplier/product combination")
    return match.iloc[0]['product_name_backend']

def prepare_product_tariffs(backend_name, user_months):
    product_tariffs = supplier_product_tariffs_df[supplier_product_tariffs_df['product_name'].str.lower() == backend_name.lower()]
    month_df = pd.DataFrame({'user_months': user_months})
    return product_tariffs.merge(month_df, how='cross')

def attach_market_indexes(tariffs_df):
    indexes_df['month'] = pd.to_datetime(indexes_df['month'], format='%b-%y')
    indexes_long = indexes_df.melt(id_vars='month', var_name='index_name', value_name='index_value')
    indexes_long['index_name'] = indexes_long['index_name'].str.lower()

    tariffs_df['index'] = tariffs_df['index'].str.lower().str.strip()
    tariffs_df['user_months'] = pd.to_datetime(tariffs_df['user_months'])

    merged = tariffs_df.merge(
        indexes_long,
        how='left',
        left_on=['index', 'user_months'],
        right_on=['index_name', 'month']
    )

    merged['index'] = merged['index_value']
    return merged.drop(columns=['index_name', 'month_y', 'index_value'])

def compute_montly_volumes(df):
    register_mapping = {
        "afname dag": "offtake_peak",
        "afname nacht": "offtake_offpeak",
        "injectie dag": "injection_peak",
        "injectie nacht": "injection_offpeak"
    }
    df['register_clean'] = df['register_clean'].replace(register_mapping)
    df['month'] = pd.to_datetime(df['month'], format="%Y-%m").dt.to_period('M').dt.to_timestamp()

    return df.groupby(['month', 'register_clean'], as_index=False)['Volume'].sum()

def compute_monthly_energy_costs(df, tariff_df):
    grouped = compute_montly_volumes(df)
    tariff_df['user_months'] = pd.to_datetime(tariff_df['user_months'])

    merged = grouped.merge(
        tariff_df,
        how='left',
        left_on=['month', 'register_clean'],
        right_on=['user_months', 'flow']
    )
    merged['month_index'] = merged['a'] * merged['index'] + merged['b']
    merged['sign'] = merged['flow'].str.contains('injection', case=False).map({True: -1, False: 1})
    merged['energy_cost'] = merged['month_index'] / 1000 * merged['Volume'] * merged['sign']
    cost_df = merged.groupby(merged['user_months'].dt.to_period('M').dt.to_timestamp())[['energy_cost']].sum().reset_index()
    cost_df.rename(columns={'user_months': 'month'}, inplace=True)

    return cost_df

def get_grid_costs(postal_code, dnb_postalcode_df, grid_and_levies_df):
    # 1. Get DNB from postal code
    postal_code = str(postal_code)
    dnb_postalcode_df['Postcode'] = dnb_postalcode_df['Postcode'].astype(str)
    dnb = dnb_postalcode_df.loc[
        dnb_postalcode_df['Postcode'] == postal_code, 'DNB Elektriciteit'
    ].iloc[0]
    grid_and_levies_df['DNB'] = grid_and_levies_df['DNB'].str.lower()
    matched_levies = grid_and_levies_df[grid_and_levies_df['DNB'] == dnb.lower()]
    return matched_levies

def compute_capacity_tariff(user_df, postal_code, dnb_postalcode_df, grid_and_levies_df):
    # 1. Get DNB from postal code
    matched_levies = get_grid_costs(postal_code, dnb_postalcode_df, grid_and_levies_df)
    # 2. Extract offtake rows and compute highest 15-min volume per month
    user_df = user_df.copy()
    user_df['month'] = user_df['datetime'].dt.to_period('M').dt.to_timestamp()
    offtake_df = user_df[user_df['register_clean'].str.startswith('offtake')]
    peak_df = offtake_df.groupby('month', as_index=False)['Volume'].max()
    peak_df.rename(columns={'Volume': 'max_quarterly_volume_kWh'}, inplace=True)

    # 3. Convert 15-min kWh to kW (x4) and extract year
    peak_df['year'] = peak_df['month'].dt.year
    peak_df['kw_peak'] = peak_df['max_quarterly_volume_kWh'] * 4
    peak_df['kw_peak'] = peak_df['kw_peak'].apply(lambda x: max(x, 2.5) if pd.notnull(x) else x)

    # 4. Merge with grid levies for the DNB and year
    peak_df = peak_df.merge(matched_levies, how='left', on='year')

    peak_df['days_in_month'] = peak_df['month'].dt.days_in_month

    # Calculate capacity cost
    peak_df['capacity_cost'] = (
        peak_df['kw_peak'] *
        peak_df['capacity_cost_[EUR/kW.day]'] *
        peak_df['days_in_month']
    )

    peak_df['data_cost'] = (peak_df['days_in_month'] * peak_df['datatariff_[EUR/day]'])

    return peak_df[["month", "kw_peak", "capacity_cost", "data_cost"]]

def compute_grid_costs(user_df, postal_code, dnb_postalcode_df, grid_and_levies_df):
    matched_levies = get_grid_costs(postal_code, dnb_postalcode_df, grid_and_levies_df)
    monthly_volumes_df = compute_montly_volumes(user_df)
    
    offtake_df = monthly_volumes_df[monthly_volumes_df['register_clean'].str.startswith('offtake')].copy()
    offtake_df['year'] = pd.to_datetime(offtake_df['month']).dt.year
    offtake_df = offtake_df.merge(
        matched_levies[['year', 'grid_cost_[EUR/MWh]', 'levies_costs_[EUR/MWh]']],
        on='year',
        how='left'
    )

    offtake_df['grid_cost'] = offtake_df['Volume'] * offtake_df['grid_cost_[EUR/MWh]'] * 0.001
    offtake_df['levies_cost'] = offtake_df['Volume'] * offtake_df['levies_costs_[EUR/MWh]'] * 0.001
    offtake_costs_df = offtake_df[['month', 'grid_cost', 'levies_cost']]
    return offtake_costs_df.groupby('month', as_index=False).sum(numeric_only=True)
    
def build_data_json(df):
    # Assuming df is the DataFrame with all cost columns
    df['month_dt'] = pd.to_datetime(df['month'])  # Ensure month column is datetime
    
    # Cost breakdown totals (rounded for output)
    costBreakdown = {
        "energy": round(df['energy_cost'].sum(), 2),
        "variableGrid": round(df['grid_cost'].sum(), 2),
        "capacityTariff": round(df['capacity_cost'].sum(), 2),
        "fixedCosts": round(df['data_cost'].sum(), 2),
        "levies": round(df['levies_cost'].sum(), 2),
        "vat": round(df['vat'].sum(), 2),
        "total": round(df['total_VATi'].sum(), 2)
    }
    
    # Monthly data
    monthlyData = []
    for _, row in df.iterrows():
        month_abbr = calendar.month_abbr[row['month_dt'].month]
        monthlyData.append({
            "month": month_abbr,
            "energy": round(row['energy_cost'], 2),
            "variableGrid": round(row['grid_cost'], 2),
            "capacityTariff": round(row['capacity_cost'], 2),
            "fixedCosts": round(row['data_cost'], 2),
            "levies": round(row['levies_cost'], 2),
            "vat": round(row['vat'], 2)
        })
    
    # Analysis period string
    first = df['month_dt'].min()
    last = df['month_dt'].max()
    analysisperiod = f"Data analyzed for {first.strftime('%b %Y')} to {last.strftime('%b %Y')}"
    
    # Final JSON-like structure
    final_output = {
        "costBreakdown": costBreakdown,
        "monthlyData": monthlyData,
        "analysisperiod": analysisperiod
    }
    
    return final_output

@app.route('/api/analyze', methods=['POST'])
def analyze():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    file = request.files['file']
    
    supplier = request.form.get('supplier', '').strip()
    product = request.form.get('product', '').strip()
    postal_code = request.form.get('postalCode', '').strip()
    
    df = load_user_meter_data(file)
    df_monthly = compute_montly_volumes(df)    

    backend_name = resolve_supplier_backend_name(supplier, product)
    earliest, latest = df['datetime'].min(), df['datetime'].max()
    user_months = pd.date_range(start=earliest.replace(day=1), end=latest.replace(day=1), freq='MS')
    
    tariffs = prepare_product_tariffs(backend_name, user_months)
    indexed_tariffs = attach_market_indexes(tariffs)
    
    energy_cost_df = compute_monthly_energy_costs(df, indexed_tariffs)
    capacity_df = compute_capacity_tariff(
        user_df=df,
        postal_code=postal_code,
        dnb_postalcode_df=dnb_postalcode_df,
        grid_and_levies_df=grid_and_levies_df
    )
    grid_costs_df = compute_grid_costs(
        user_df=df,
        postal_code=postal_code,
        dnb_postalcode_df=dnb_postalcode_df,
        grid_and_levies_df=grid_and_levies_df
    )
    merged_df = pd.merge(energy_cost_df, capacity_df, on='month', how='outer')
    merged_df = pd.merge(merged_df, grid_costs_df, on='month', how='outer')
    merged_df = merged_df.sort_values('month').reset_index(drop=True)
    merged_df['total_VATe'] = merged_df[['capacity_cost', 'data_cost', 'energy_cost', 'grid_cost', 'levies_cost']].sum(axis=1)
    merged_df['vat'] = merged_df['total_VATe'] * 0.06
    merged_df['total_VATi'] = merged_df['total_VATe'] + merged_df['vat']
    data = build_data_json(merged_df)

    return jsonify(data), 200


@app.route('/')
def index():
    return "Energy backend is running! xxxx loic"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
