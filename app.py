import sys
import numpy as np
import pandas as pd
import dash
from dash import Dash, dcc, html, Input, Output, State, dash_table
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
import plotly.express as px
import plotly.io as pio
import re
import base64
import io
import datetime as dt
import os
import time
import json
from functools import lru_cache

# SQLAlchemy imports
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, TIMESTAMP, select, func, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

import logging

logging.basicConfig(filename='app_errors.log', level=logging.ERROR, 
                    format='%(asctime)s %(levelname)s: %(message)s')

def log_error(message):
    print(message)  # Print to console
    logging.error(message)  # Log to file

# Environment variables
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://qhbw_sql_vx26_user:glwGtA9yoGUtBrow7YdM9Ps80xNkGQIL@dpg-cqli5h08fa8c73aurv40-a.oregon-postgres.render.com/qhbw_sql_vx26')

# Initialize SQLAlchemy engine with connection pooling
engine = create_engine(DATABASE_URL, poolclass=QueuePool, pool_size=10, max_overflow=20, pool_pre_ping=True)

# Define metadata
metadata = MetaData()

# Define database tables
processed_data = Table(
    'processed_data', metadata,
    Column('id', Integer, primary_key=True),
    Column('hole_id', String, index=True),
    Column('stage', String, index=True),
    Column('TIMESTAMP', TIMESTAMP, index=True),
    Column('RECORD', Float),
    Column('CumTimeMin', Float),
    Column('flow', Float),
    Column('AvgFlow', Float),
    Column('volume', Float),
    Column('gaugePressure', Float),
    Column('AvgGaugePressure', Float),
    Column('effPressure', Float),
    Column('AvgEffectivePressure', Float),
    Column('Lugeon', Float),
    Column('Batt_V', Float),
    Column('PTemp', Float),
    Column('holeNum', Float),
    Column('mixNum', Float),
    Column('waterTable', Float),
    Column('stageTop', Float),
    Column('stageBottom', Float),
    Column('gaugeHeight', Float),
    Column('waterDepth', Float),
    Column('holeAngle', Float),
    Column('vmarshGrout', Float),
    Column('vmarshWater', Float),
    Column('Notes', String)
)

# Create tables if they don't exist
metadata.create_all(engine)

@lru_cache(maxsize=None)
def get_unique_values(table_name, column_name, hole_id=None):
    try:
        with engine.connect() as connection:
            if column_name == 'hole_id':
                query = text("SELECT DISTINCT hole_id FROM processed_data ORDER BY hole_id")
                result = connection.execute(query)
                return [row[0] for row in result]
            elif column_name == 'stage':
                if hole_id:
                    query = text("SELECT DISTINCT stage FROM processed_data WHERE hole_id = :hole_id ORDER BY stage")
                    result = connection.execute(query, {"hole_id": hole_id})
                    return [row[0] for row in result]
                else:
                    return []
    except Exception as e:
        log_error(f"Error getting unique values: {e}")
        return []

def extract_file_details(file_name):
    try:
        pattern = r'([PS]\d{4})_(S\d+(?:&\d+)?)'
        match = re.search(pattern, file_name)
        if match:
            hole_id, stage = match.groups()
            order = 'P' if hole_id.startswith('P') else 'S'
            return hole_id, stage, order
    except Exception as e:
        log_error(f"Error extracting file details from {file_name}: {e}")
    return None, None, None

def clean_data(data):
    try:
        headers = [
            'TIMESTAMP', 'RECORD', 'CumTimeMin', 'flow', 'AvgFlow', 'volume', 'gaugePressure',
            'AvgGaugePressure', 'effPressure', 'AvgEffectivePressure', 'Lugeon', 'Batt_V', 'PTemp', 
            'holeNum', 'mixNum', 'waterTable', 'stageTop', 'stageBottom', 'gaugeHeight', 
            'waterDepth', 'holeAngle', 'vmarshGrout', 'vmarshWater', 'Notes'
        ]
        
        data = data.drop([0, 1, 2]).reset_index(drop=True)
        data.columns = headers
        data = data.drop(data.index[0])

        data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'], errors='coerce')
        data = data.dropna(subset=['TIMESTAMP'])

        numeric_columns = ['flow', 'AvgFlow', 'volume', 'gaugePressure', 'AvgGaugePressure', 
                           'effPressure', 'AvgEffectivePressure', 'Lugeon', 'Batt_V', 'PTemp', 
                           'holeNum', 'mixNum', 'waterTable', 'stageTop', 'stageBottom', 
                           'gaugeHeight', 'waterDepth', 'holeAngle', 'vmarshGrout', 'vmarshWater']
        data[numeric_columns] = data[numeric_columns].apply(pd.to_numeric, errors='coerce')

        return data
    except Exception as e:
        log_error(f"Error cleaning data: {e}")
        return None

def parse_contents(contents, filename):
    content_type, content_string = contents.split(',')
    decoded = base64.b64decode(content_string)
    try:
        if 'csv' in filename:
            df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
        elif 'xls' in filename or 'xlsx' in filename:
            df = pd.read_excel(io.BytesIO(decoded), sheet_name='Raw Data', engine='openpyxl' if 'xlsx' in filename else 'xlrd')
        else:
            raise ValueError("Unsupported file type")
        cleaned_data = clean_data(df)
        mixes_and_marsh = track_mixes_and_marsh_values(cleaned_data)
        return cleaned_data, mixes_and_marsh
    except Exception as e:
        log_error(f"Error parsing file: {e}")
        return None, None

def store_processed_data(df, hole_id, stage):
    try:
        with engine.begin() as connection:
            df['hole_id'] = hole_id
            df['stage'] = stage
            df['Notes'] = df['Notes'].replace({'NaN': '', 'nan': ''}).fillna('')
            df.to_sql('processed_data', connection, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f"Data for {hole_id} {stage} stored successfully.")
    except Exception as e:
        log_error(f"Error storing processed data: {e}")

def retrieve_processed_data(hole_id, stage):
    try:
        query = text("""
            SELECT * FROM processed_data 
            WHERE hole_id = :hole_id AND stage = :stage
            ORDER BY "TIMESTAMP"
        """)
        df = pd.read_sql_query(query, engine, params={"hole_id": hole_id, "stage": stage})
        
        if df.empty:
            print(f"No data found for hole_id: {hole_id} and stage: {stage}")
        else:
            print(f"Retrieved {len(df)} rows for hole_id: {hole_id} and stage: {stage}")
        
        # Clean the Notes column
        if 'Notes' in df.columns:
            df['Notes'] = df['Notes'].replace({'NaN': None, 'nan': None}).fillna('')
        
        return df
    except Exception as e:
        log_error(f"Error retrieving processed data: {e}")
        return None

def track_mixes_and_marsh_values(data):
    mix_counts = {'A': 0, 'B': 0, 'C': 0, 'D': 0}
    marsh_values = {'A': None, 'B': None, 'C': None, 'D': None}
    current_mix = 'A'

    for _, row in data.iterrows():
        mix_num = row['mixNum']
        marsh_value = row['vmarshGrout']

        if marsh_values[current_mix] is None:
            marsh_values[current_mix] = marsh_value
            mix_counts[current_mix] += 1
        elif marsh_value != marsh_values[current_mix]:
            mix_counts[current_mix] += 1
            marsh_values[current_mix] = marsh_value

        if mix_num == 3 and current_mix == 'A':
            current_mix = 'B'
        elif mix_num == 4 and current_mix == 'B':
            current_mix = 'C'
        elif mix_num == 5 and current_mix == 'C':
            current_mix = 'D'

    zero_flow_interval = calculate_cumulative_zero_flow(data)

    return {
        'Mix A': mix_counts['A'],
        'Mix B': mix_counts['B'],
        'Mix C': mix_counts['C'],
        'Mix D': mix_counts['D'],
        'Cumulative Zero Flow': zero_flow_interval
    }

def identify_marsh_changes(data):
    try:
        marsh_changes = data[data['vmarshGrout'].diff() != 0]
        return marsh_changes[['TIMESTAMP', 'mixNum', 'vmarshGrout', 'flow']]
    except Exception as e:
        log_error(f"Error identifying Marsh changes: {e}")
        return pd.DataFrame(columns=['TIMESTAMP', 'mixNum', 'vmarshGrout', 'flow'])

def extract_notes(data):
    try:
        if 'Notes' not in data.columns:
            return pd.DataFrame(columns=['Timestamp', 'Note'])

        notes_data = data[['TIMESTAMP', 'Notes']].copy()
        notes_data = notes_data[
            (notes_data['Notes'].notna()) & 
            (notes_data['Notes'] != '') & 
            (notes_data['Notes'].astype(str) != 'NaN')
        ]
        
        if notes_data.empty:
            return pd.DataFrame(columns=['Timestamp', 'Note'])

        notes_data.columns = ['Timestamp', 'Note']
        notes_data['Timestamp'] = pd.to_datetime(notes_data['Timestamp']).dt.strftime('%H:%M')
        return notes_data
    except Exception as e:
        log_error(f"Error extracting notes: {e}")
        return pd.DataFrame(columns=['Timestamp', 'Note'])

def update_notes_table(notes_data):
    if notes_data is None or notes_data.empty:
        return "No operator notes available."
    
    valid_notes = [
        (row['Timestamp'], row['Note']) for _, row in notes_data.iterrows() 
        if pd.notna(row['Note']) and row['Note'] != '' and str(row['Note']) != 'NaN'
    ]
    
    if not valid_notes:
        return "No valid operator notes available."
    
    return "\n".join(f"{timestamp} - {note}" for timestamp, note in valid_notes)

def generate_interactive_graph(data):
    try:
        if data is None or data.empty:
            return None, None, None, None, None, None, None

        marsh_changes = identify_marsh_changes(data)
        notes_data = extract_notes(data)

        hole_id = data['holeNum'].iloc[0] if 'holeNum' in data.columns else 'Unknown'
        start_time = data['TIMESTAMP'].min()
        data['ElapsedMinutes'] = (data['TIMESTAMP'] - start_time).dt.total_seconds() / 60

        fig = go.Figure()

        mix_colors = {2: 'blue', 3: 'cyan', 4: 'magenta', 5: 'orange'}
        mix_labels = {2: 'A', 3: 'B', 4: 'C', 5: 'D'}
        for mix, color in mix_colors.items():
            mix_data = data[data['mixNum'] == mix]
            if not mix_data.empty:
                add_trace(fig, mix_data, f'Flow Rate Mix {mix_labels[mix]}', 'flow', color)
                add_trace(fig, mix_data, f'Effective Pressure Mix {mix_labels[mix]}', 'effPressure', 'green', yaxis='y2')

        mix_changes = data[data['mixNum'].diff().abs() > 0]
        for _, row in mix_changes.iterrows():
            mix_change_min = (row['TIMESTAMP'] - start_time).total_seconds() / 60
            fig.add_vline(x=mix_change_min, line=dict(color='red', width=2, dash='dash'))

        for _, row in marsh_changes.iterrows():
            marsh_change_min = (row['TIMESTAMP'] - start_time).total_seconds() / 60
            fig.add_trace(go.Scatter(
                x=[marsh_change_min],
                y=[row['flow']],
                mode='markers+text',
                marker=dict(color='black', size=8),
                text=[f"{row['vmarshGrout']:.1f}"],
                textposition='top center'
            ))

        time_ticks = pd.date_range(start=start_time, end=data['TIMESTAMP'].max(), freq='15T')
        fig.update_xaxes(
            tickvals=[(t - start_time).total_seconds() / 60 for t in time_ticks],
            ticktext=[t.strftime('%H:%M') for t in time_ticks],
            title='Time Elapsed (Minutes)'
        )

        fig.update_layout(
            title='Flow Rate and Effective Pressure vs Time for Mixes',
            yaxis=dict(title='Flow Rate (L/min)', side='left'),
            yaxis2=dict(title='Effective Pressure (bar)', overlaying='y', side='right'),
            hovermode='x unified'
        )

        # Create temperature plot
        temp_fig = go.Figure()
        add_trace(temp_fig, data, 'Temperature', 'PTemp', 'red')
        temp_fig.update_layout(
            title='Temperature vs Time',
            xaxis_title='Time Elapsed (Minutes)',
            yaxis_title='Temperature (°C)'
        )
        temp_fig.update_xaxes(
            tickvals=[(t - start_time).total_seconds() / 60 for t in time_ticks],
            ticktext=[t.strftime('%H:%M') for t in time_ticks]
        )

        # Create 3D Scatter Plot
        scatter_3d_fig = go.Figure(data=go.Scatter3d(
            x=data['Lugeon'],
            y=data['flow'],
            z=data['effPressure'],
            mode='markers',
            marker=dict(
                size=5,
                color=data['mixNum'],
                colorscale=['blue', 'cyan', 'magenta', 'orange'],
                opacity=0.8,
                colorbar=dict(title="Mix Type"),
                cmin=2,
                cmax=5
            ),
            text=[f"Mix {mix_labels[m]}" for m in data['mixNum']],
            hoverinfo='text'
        ))
        scatter_3d_fig.update_layout(
            title='3D Scatter Plot: Lugeon vs Flow vs Effective Pressure',
            scene=dict(
                xaxis_title='Lugeon',
                yaxis_title='Flow (L/min)',
                zaxis_title='Effective Pressure (bar)'
            )
        )

        # Create Pie Chart for mix volumes
        mix_volumes = calculate_grout_and_mix_volumes(data)
        pie_fig = go.Figure(data=[go.Pie(
            labels=['Mix A', 'Mix B', 'Mix C', 'Mix D'], 
            values=[mix_volumes[0], mix_volumes[1], mix_volumes[2], mix_volumes[3]],
            hole=.3,
            pull=[0.1, 0.1, 0.1, 0.1]
        )])
        pie_fig.update_layout(title='Mix Volumes')

        # Create Lugeon vs Effective Pressure plot
        lugeon_fig = go.Figure()
        add_trace(lugeon_fig, data, 'Lugeon', 'Lugeon', 'black')
        add_trace(lugeon_fig, data, 'Effective Pressure', 'effPressure', 'green', yaxis='y2')

        # Detect refusal trend
        data['rolling_lugeon'] = data['Lugeon'].rolling(window=12, min_periods=1).mean()  # 2 minutes (assuming 10-second intervals)
        refusal_periods = data[data['rolling_lugeon'] <= 3.3]

        if not refusal_periods.empty:
            refusal_start = refusal_periods.index[0]
            refusal_end = refusal_periods.index[-1]
            lugeon_fig.add_shape(
                type="rect",
                x0=data.loc[refusal_start, 'ElapsedMinutes'],
                x1=data.loc[refusal_end, 'ElapsedMinutes'],
                y0=0,
                y1=data['Lugeon'].max(),
                fillcolor="red",
                opacity=0.2,
                layer="below",
                line_width=0,
            )
            lugeon_fig.add_annotation(
                x=(data.loc[refusal_start, 'ElapsedMinutes'] + data.loc[refusal_end, 'ElapsedMinutes']) / 2,
                y=data['Lugeon'].max(),
                text="Refusal trend detected",
                showarrow=True,
                arrowhead=2,
                arrowsize=1,
                arrowwidth=2,
                arrowcolor="red",
                font=dict(size=12, color="red"),
                align="center",
            )

        lugeon_fig.update_layout(
            title='Lugeon and Effective Pressure vs Time',
            xaxis_title='Time Elapsed (Minutes)',
            yaxis=dict(title='Lugeon', side='left'),
            yaxis2=dict(title='Effective Pressure (bar)', overlaying='y', side='right'),
            hovermode='x unified'
        )
        lugeon_fig.update_xaxes(
            tickvals=[(t - start_time).total_seconds() / 60 for t in time_ticks],
            ticktext=[t.strftime('%H:%M') for t in time_ticks]
        )

        return fig, temp_fig, scatter_3d_fig, pie_fig, data, notes_data, lugeon_fig
    except Exception as e:
        log_error(f"Error generating interactive graph: {e}")
        return None, None, None, None, None, None, None

def add_trace(fig, data, name, y_col, color, yaxis='y'):
    fig.add_trace(go.Scatter(
        x=data['ElapsedMinutes'],
        y=data[y_col],
        mode='lines+markers',
        name=name,
        line=dict(color=color),
        text=[f"Time: {row['TIMESTAMP'].strftime('%H:%M')}, {name}: {row[y_col]:.1f}, Flow: {row['flow']:.1f}, Eff Pressure: {row['effPressure']:.1f}, Gauge Pressure: {row['gaugePressure']:.1f}, Lugeon: {row['Lugeon']:.1f}, Volume: {row['volume']:.1f}" 
              for _, row in data.iterrows()],
        hoverinfo='text',
        yaxis=yaxis
    ))

def calculate_cumulative_zero_flow(data):
    try:
        # Convert TIMESTAMP to datetime if it's not already
        data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])
        
        # Create a mask for zero flow
        zero_flow_mask = data['flow'] == 0
        
        # Create groups of consecutive zero flow periods
        zero_flow_groups = (zero_flow_mask != zero_flow_mask.shift()).cumsum()[zero_flow_mask]
        
        # Calculate the duration of each zero flow period
        zero_flow_durations = data[zero_flow_mask].groupby(zero_flow_groups)['TIMESTAMP'].agg(['first', 'last'])
        zero_flow_durations['duration'] = (zero_flow_durations['last'] - zero_flow_durations['first']).dt.total_seconds() / 3600
        
        # Sum durations of periods longer than 10 minutes (0.1667 hours)
        cumulative_zero_flow = zero_flow_durations[zero_flow_durations['duration'] > 0.1667]['duration'].sum()
        
        # Ensure the result is not negative
        cumulative_zero_flow = max(0, cumulative_zero_flow)
        
        print(f"Debug: Cumulative zero flow calculated as {cumulative_zero_flow:.2f} hours")
        
        return cumulative_zero_flow
    except Exception as e:
        log_error(f"Error calculating cumulative zero flow: {e}")
        return 0

def calculate_grout_and_mix_volumes(data):
    try:
        cumulative_grout = data['volume'].iloc[-1]
        mix_volumes = data.groupby('mixNum')['volume'].last().diff().fillna(data.groupby('mixNum')['volume'].last())
        mix_a_volume = mix_volumes.get(2, 0)
        mix_b_volume = mix_volumes.get(3, 0)
        mix_c_volume = mix_volumes.get(4, 0)
        mix_d_volume = mix_volumes.get(5, 0)

        return mix_a_volume, mix_b_volume, mix_c_volume, mix_d_volume, cumulative_grout
    except Exception as e:
        log_error(f"Error calculating grout and mix volumes: {e}")
        return 0, 0, 0, 0, 0

def update_injection_details(data, selected_stage, selected_hole_id):
    try:
        # Data validation checks
        print(f"Debug: Data shape: {data.shape}")
        print(f"Debug: Columns: {data.columns.tolist()}")
        print(f"Debug: Flow column dtype: {data['flow'].dtype}")
        print(f"Debug: Flow column range: {data['flow'].min()} to {data['flow'].max()}")
        print(f"Debug: Timestamp range: {data['TIMESTAMP'].min()} to {data['TIMESTAMP'].max()}")
        
        # Check for negative flows
        negative_flows = data[data['flow'] < 0]
        if not negative_flows.empty:
            print(f"Warning: Negative flows detected: {len(negative_flows)} rows")

        # Check for duplicate timestamps
        duplicate_timestamps = data[data.duplicated('TIMESTAMP', keep=False)]
        if not duplicate_timestamps.empty:
            print(f"Warning: Duplicate timestamps detected: {len(duplicate_timestamps)} rows")

        data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'])
        non_zero_flow = data[data['flow'] > 0]
        first_non_zero_flow_time = non_zero_flow['TIMESTAMP'].min()
        last_timestamp = data['TIMESTAMP'].max()

        stage_top = data.loc[data['TIMESTAMP'].idxmin(), 'stageTop']
        stage_bottom = data.loc[data['TIMESTAMP'].idxmin(), 'stageBottom']
        stage_length = stage_bottom - stage_top

        first_date = first_non_zero_flow_time.strftime('%Y-%m-%d')
        first_time = first_non_zero_flow_time.strftime('%H:%M:%S')
        last_date = last_timestamp.strftime('%Y-%m-%d')
        last_time = last_timestamp.strftime('%H:%M:%S')

        mix_a_volume, mix_b_volume, mix_c_volume, mix_d_volume, cumulative_volume = calculate_grout_and_mix_volumes(data)

        total_grouting_time = (last_timestamp - first_non_zero_flow_time).total_seconds() / 3600
        zero_flow_interval = calculate_cumulative_zero_flow(data)
        net_grouting_time = total_grouting_time - zero_flow_interval

        print(f"Debug: Total grouting time: {total_grouting_time:.2f} hours")
        print(f"Debug: Zero flow interval: {zero_flow_interval:.2f} hours")
        print(f"Debug: Net grouting time: {net_grouting_time:.2f} hours")

        water_depth = data['waterDepth'].iloc[0]
        v_marsh_water = data['vmarshWater'].iloc[0]

        details = [
            html.H2("Grout Injection Summary"),
            html.B("Hole ID: "), html.Span(f"{selected_hole_id}", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Stage: "), html.Span(f"{selected_stage}", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Commencement Date/ Time: "), html.Span(f"{first_date} at {first_time}", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Completion Date/ Time: "), html.Span(f"{last_date} at {last_time}", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Stage Top: "), html.Span(f"{stage_top} meters", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Stage Bottom: "), html.Span(f"{stage_bottom} meters", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Stage Length: "), html.Span(f"{stage_length:.2f} meters", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B(f"Mix A: "), html.Span(f"{mix_a_volume:.2f} L", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B(f"Mix B: "), html.Span(f"{mix_b_volume:.2f} L", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B(f"Mix C: "), html.Span(f"{mix_c_volume:.2f} L", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B(f"Mix D: "), html.Span(f"{mix_d_volume:.2f} L", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B(f"Cumulative Volume: "), html.Span(f"{cumulative_volume:.2f} L", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Total Grouting Time: "), html.Span(f"{total_grouting_time:.2f} hours", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Net Grouting Time: "), html.Span(f"{net_grouting_time:.2f} hours", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Cumulative Zero Flow: "), html.Span(f"{zero_flow_interval:.2f} hours", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("Water Table: "), html.Span(f"{water_depth:.2f} meters" if water_depth is not None else "N/A", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
            html.B("V water: "), html.Span(f"{v_marsh_water:.2f} {'(reported correctly)' if 25.6 <= v_marsh_water <= 26.8 else ''}" if v_marsh_water is not None else "N/A", style={'color': 'red', 'font-weight': 'bold'}),
            html.Br(),
        ]

        return html.Div(details)
    except Exception as e:
        log_error(f"Error updating injection details: {e}")
        return "Error updating injection details"

def apply_tma(data, window_size=5):
    weights = np.arange(1, window_size + 1)
    weights = np.concatenate([weights, weights[:-1][::-1]])
    weights = weights / np.sum(weights)
    
    data['flow_tma'] = data['flow'].rolling(window=len(weights), center=True).apply(lambda x: np.sum(weights * x), raw=False)
    data['effPressure_tma'] = data['effPressure'].rolling(window=len(weights), center=True).apply(lambda x: np.sum(weights * x), raw=False)
    
    return data

# Load and encode the Gecko logo
encoded_logo = None
try:
    with open('Lizard_Logo.jpg', 'rb') as f:
        encoded_logo = base64.b64encode(f.read()).decode('ascii')
except FileNotFoundError:
    print("Logo file 'Lizard_Logo.jpg' not found. The app will run without the logo.")

# Initialize Dash app
app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server

# Define the layout for the app
app.layout = html.Div([
    html.Div([
        html.Div([
            html.H1("Grout Gecko V-1.0", style={'fontSize': '48px', 'fontWeight': 'bold', 'color': 'black', 'margin': '0'}),
            html.H3("QHBW Grouting Data QC, Plotter, and Deep Analysis Tool", style={'margin': '0'})
        ], style={'flex': '1'}),
        html.Img(src=f'data:image/jpg;base64,{encoded_logo}', style={'height': '100px', 'width': 'auto'}) if encoded_logo else html.Div()
    ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'space-between', 'marginBottom': '20px'}),
     
    html.Button('Bug Report/Feedback', id='bug-report-button', n_clicks=0, style={'marginBottom': '20px'}),

    html.Div([
        html.Div([
            html.Label('Hole ID'),
            dcc.Dropdown(id='hole-id-dropdown', placeholder="Select Hole ID")
        ], style={'width': '45%', 'display': 'inline-block', 'marginRight': '10%'}),
        
        html.Div([
            html.Label('Stage'),
            dcc.Dropdown(id='stage-dropdown', placeholder="Select Stage")
        ], style={'width': '45%', 'display': 'inline-block'}),
    ], style={'marginBottom': '20px'}),
    
    dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Click Here to upload and Analysis your RTM grouting data'
        ]),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'margin': '10px 0'
        },
        multiple=False
    ),
    
    html.Div(id='upload-confirmation', style={'marginTop': 10, 'color': 'green'}),
    html.Div(id='error-message', style={'marginTop': 10, 'color': 'red'}),
    
    html.Button('Run Tool', id='run-tool-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20}),
    html.Button('Load Data', id='load-data-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}),
    html.Button('Upload to Database (Admin only)', id='upload-db-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}, disabled=True),
    html.Button('Show/Hide Temperature', id='toggle-temperature-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}),
    html.Button('Show/Hide 3D Scatter', id='toggle-3d-scatter-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}),
    html.Button('Show/Hide Pie Chart', id='toggle-pie-chart-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}),
    html.Button('Show/Hide Noise Reduction (MA)', id='toggle-ma-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}),
    html.Button('Show/Hide Lugeon', id='toggle-lugeon-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}),
    
    dcc.Loading(
        id="loading",
        type="default",
        children=[
            html.Div(id='processing-message', style={'textAlign': 'center', 'fontSize': '18px', 'marginTop': '20px'}),
            dcc.Graph(id='interactive-graph'),
            dcc.Graph(id='temperature-graph', style={'display': 'none'}),
            dcc.Graph(id='scatter-3d-plot', style={'display': 'none'}),
            dcc.Graph(id='pie-chart', style={'display': 'none'}),
            dcc.Graph(id='ma-graph', style={'display': 'none'}),
            dcc.Graph(id='lugeon-graph', style={'display': 'none'}),
        ]
    ),
    
    html.Div(id='clicked-point-data', style={'marginTop': 20, 'marginBottom': 20, 'fontWeight': 'bold'}),
    
    html.Div([
        html.Div([
            dash_table.DataTable(
                id='recipe-table',
                columns=[
                    {'name': col, 'id': col} for col in ['Recipe', 'Marsh', 'T. Start', 'T. Finish', 'Flow', 'Pe', 'Gauge', 'Gul', 'Volume']
                ],
                data=[{col: '' for col in ['Recipe', 'Marsh', 'T. Start', 'T. Finish', 'Flow', 'Pe', 'Gauge', 'Gul', 'Volume']} for _ in range(5)],
                editable=True,
                style_data_conditional=[{
                    'if': {'column_id': c},
                    'textAlign': 'center',
                    'fontFamily': 'monospace',
                } for c in ['Recipe', 'Marsh', 'T. Start', 'T. Finish', 'Flow', 'Pe', 'Gauge', 'Gul', 'Volume']],
                style_header={
                    'backgroundColor': 'rgb(230, 230, 230)',
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                },
                style_cell={
                    'height': '40px',
                    'minWidth': '80px', 'width': '80px', 'maxWidth': '80px',
                    'whiteSpace': 'normal'
                }
            )
        ], style={'width': '100%', 'display': 'inline-block', 'verticalAlign': 'top'}),
    ], style={'display': 'flex', 'marginTop': '20px'}),

    html.Div(id='injection-details', style={'marginTop': '20px'}),
    
    html.Div([
        html.H3("Mixes Count (Approximate)"),
        html.Div(id='mix-summary', style={'marginTop': 10, 'whiteSpace': 'pre-wrap'}),
        html.Hr(style={'borderTop': '2px solid #333', 'width': '100%'}),
        html.H3("Observations/Errors:"),
        html.Div(id='error-summary', style={'marginTop': 10, 'whiteSpace': 'pre-wrap', 'color': 'red'}),
        html.Div(id='giv-operator-notes', style={'marginTop': 20, 'whiteSpace': 'pre-wrap'}),
    ]),
    
    html.Div([
        html.Label('QC by:'),
        dcc.Input(id='qc-by-input', type='text', placeholder="Enter QC personnel name")
    ], style={'marginTop': '20px'}),
    
    html.Button('Print Report', id='print-report-button', n_clicks=0, style={'marginTop': 20}),
])

# Callback to populate dropdowns
@app.callback(
    [Output('hole-id-dropdown', 'options'),
     Output('stage-dropdown', 'options')],
    [Input('hole-id-dropdown', 'value')]
)
def update_dropdowns(selected_hole_id):
    hole_ids = get_unique_values('processed_data', 'hole_id')
    stages = get_unique_values('processed_data', 'stage', selected_hole_id) if selected_hole_id else []
    
    return [{'label': id, 'value': id} for id in hole_ids], [{'label': stage, 'value': stage} for stage in stages]

# Callback to display clicked point data
@app.callback(
    Output('clicked-point-data', 'children'),
    [Input('interactive-graph', 'clickData')]
)
def display_click_data(clickData):
    if clickData is None:
        return "Click on a point to see details"
    
    point = clickData['points'][0]
    text_parts = point['text'].split(', ')
    timestamp = text_parts[0].split(': ')[1]
    flow = text_parts[2].split(': ')[1]
    eff_pressure = text_parts[3].split(': ')[1]
    gauge_pressure = text_parts[4].split(': ')[1]
    lugeon = text_parts[5].split(': ')[1]
    volume = text_parts[6].split(': ')[1]
    
    return f"Flow: {flow}, Effective Pressure: {eff_pressure}, Gauge Pressure: {gauge_pressure}, Lugeon: {lugeon}, Volume: {volume}, Timestamp: {timestamp}"

# Main callback to update graph and details
@app.callback(
    [Output('upload-confirmation', 'children'),
     Output('error-message', 'children'),
     Output('interactive-graph', 'figure'),
     Output('temperature-graph', 'figure'),
     Output('scatter-3d-plot', 'figure'),
     Output('pie-chart', 'figure'),
     Output('ma-graph', 'figure'),
     Output('lugeon-graph', 'figure'),
     Output('injection-details', 'children'),
     Output('mix-summary', 'children'),
     Output('error-summary', 'children'),
     Output('giv-operator-notes', 'children'),
     Output('processing-message', 'children')],
    [Input('upload-data', 'contents'),
     Input('run-tool-button', 'n_clicks'),
     Input('load-data-button', 'n_clicks'),
     Input('hole-id-dropdown', 'value'),
     Input('stage-dropdown', 'value')],
    [State('upload-data', 'filename')]
)
def update_and_run_tool(contents, run_clicks, load_clicks, hole_id, stage, filename):
    ctx = dash.callback_context
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger_id == 'upload-data' and contents:
        try:
            df, mixes_and_marsh = parse_contents(contents, filename)
            if df is None:
                raise ValueError("Error parsing file contents")

            fig, temp_fig, scatter_3d_fig, pie_fig, data, notes_data, lugeon_fig = generate_interactive_graph(df)
            
            # Generate MA graph
            df_ma = apply_tma(df)
            ma_fig = go.Figure()
            add_trace(ma_fig, df_ma, 'Flow Rate (MA)', 'flow_tma', 'blue')
            add_trace(ma_fig, df_ma, 'Effective Pressure (MA)', 'effPressure_tma', 'green', yaxis='y2')
            ma_fig.update_layout(
                title='Noise Reduction (Moving Average)',
                yaxis=dict(title='Flow Rate (L/min)', side='left'),
                yaxis2=dict(title='Effective Pressure (bar)', overlaying='y', side='right'),
                hovermode='x unified'
            )

            injection_details = update_injection_details(df, stage, hole_id)
            mix_summary = html.Div([
                html.P(f"Mix {mix}: {count} times") for mix, count in mixes_and_marsh.items() if mix not in ['Cumulative Zero Flow']
            ])
            
            stage_length = df['stageBottom'].iloc[0] - df['stageTop'].iloc[0]
            error_summary = []
            if stage_length < 6:
                error_summary.append("Short stage length detected")
            elif stage_length > 6:
                error_summary.append("Long stage length detected")
            error_summary.extend([html.Span(error, style={'color': 'red'}) for error in mixes_and_marsh.get('Errors', [])] or ["NA"])
            error_summary = html.Div(error_summary)

            giv_operator_notes = html.Div([
                html.H3("GIV Operator Notes:"),
                html.Pre(update_notes_table(notes_data))
            ]) if not notes_data.empty else ""

            return (f"File '{filename}' processed successfully", "",
                    fig, temp_fig, scatter_3d_fig, pie_fig, ma_fig, lugeon_fig, injection_details, mix_summary, error_summary, giv_operator_notes, "")
        except Exception as e:
            error_message = f"Error processing file: {str(e)}"
            log_error(error_message)
            return "", error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""

    elif trigger_id == 'load-data-button' and hole_id and stage:
        try:
            print(f"Attempting to load data for hole_id: {hole_id}, stage: {stage}")
            data = retrieve_processed_data(hole_id, stage)
            if data is None or data.empty:
                error_message = f"No data found for Hole ID: {hole_id} and Stage: {stage}"
                log_error(error_message)
                return "", error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""
            
            print(f"Data retrieved successfully. Shape: {data.shape}")
            mixes_and_marsh = track_mixes_and_marsh_values(data)
            
            fig, temp_fig, scatter_3d_fig, pie_fig, _, notes_data, lugeon_fig = generate_interactive_graph(data)
            
            # Generate MA graph
            data_ma = apply_tma(data)
            ma_fig = go.Figure()
            add_trace(ma_fig, data_ma, 'Flow Rate (MA)', 'flow_tma', 'blue')
            add_trace(ma_fig, data_ma, 'Effective Pressure (MA)', 'effPressure_tma', 'green', yaxis='y2')
            ma_fig.update_layout(
                title='Noise Reduction (Moving Average)',
                yaxis=dict(title='Flow Rate (L/min)', side='left'),
                yaxis2=dict(title='Effective Pressure (bar)', overlaying='y', side='right'),
                hovermode='x unified'
            )
            
            injection_details = update_injection_details(data, stage, hole_id)
            mix_summary = html.Div([
                html.P(f"Mix {mix}: {count} times") for mix, count in mixes_and_marsh.items() if mix not in ['Cumulative Zero Flow']
            ])
            
            stage_length = data['stageBottom'].iloc[0] - data['stageTop'].iloc[0]
            error_summary = []
            if stage_length < 6:
                error_summary.append("Short stage length detected")
            elif stage_length > 6:
                error_summary.append("Long stage length detected")
            error_summary.extend([html.Span(error, style={'color': 'red'}) for error in mixes_and_marsh.get('Errors', [])] or ["NA"])
            error_summary = html.Div(error_summary)

            giv_operator_notes = html.Div([
                html.H3("GIV Operator Notes:"),
                html.Pre(update_notes_table(notes_data))
            ]) if not notes_data.empty else ""

            return f"Data for {hole_id} {stage} loaded successfully", "", fig, temp_fig, scatter_3d_fig, pie_fig, ma_fig, lugeon_fig, injection_details, mix_summary, error_summary, giv_operator_notes, ""
        except Exception as e:
            error_message = f"Error loading data: {str(e)}"
            log_error(error_message)
            return "", error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""

    raise PreventUpdate

# Callbacks to show/hide additional plots
@app.callback(
    Output('temperature-graph', 'style'),
    [Input('toggle-temperature-button', 'n_clicks')]
)
def toggle_temperature_graph(n_clicks):
    if n_clicks is None:
        return {'display': 'none'}
    return {'display': 'block' if n_clicks % 2 == 1 else 'none'}

@app.callback(
    Output('scatter-3d-plot', 'style'),
    [Input('toggle-3d-scatter-button', 'n_clicks')]
)
def toggle_3d_scatter_plot(n_clicks):
    if n_clicks is None:
        return {'display': 'none'}
    return {'display': 'block' if n_clicks % 2 == 1 else 'none'}

@app.callback(
    Output('pie-chart', 'style'),
    [Input('toggle-pie-chart-button', 'n_clicks')]
)
def toggle_pie_chart(n_clicks):
    if n_clicks is None:
        return {'display': 'none'}
    return {'display': 'block' if n_clicks % 2 == 1 else 'none'}

@app.callback(
    Output('ma-graph', 'style'),
    [Input('toggle-ma-button', 'n_clicks')]
)
def toggle_ma_graph(n_clicks):
    if n_clicks is None:
        return {'display': 'none'}
    return {'display': 'block' if n_clicks % 2 == 1 else 'none'}

@app.callback(
    Output('lugeon-graph', 'style'),
    [Input('toggle-lugeon-button', 'n_clicks')]
)
def toggle_lugeon_graph(n_clicks):
    if n_clicks is None:
        return {'display': 'none'}
    return {'display': 'block' if n_clicks % 2 == 1 else 'none'}

# Callback for bug report/feedback button
@app.callback(
    Output("bug-report-button", "n_clicks"),
    Input("bug-report-button", "n_clicks"),
)
def open_email_client(n_clicks):
    if n_clicks > 0:
        import webbrowser
        mailto_link = "mailto:Ehab.Mdallal@wsp.com?subject=Grout Gecko V-1.0 Feedback&body=This app is designed for the QHBW project to perform deep analysis, identify human errors, and collect high-quality data, enhancing the QHBW Leapfrog model. Please note that this tool is still under development. Bugs and inaccuracies might be found; your feedback is appreciated.%0D%0ARegards"
        webbrowser.open(mailto_link)
    return 0

# Callback for printing the report
@app.callback(
    Output('print-report-button', 'n_clicks'),
    Input('print-report-button', 'n_clicks'),
    [State('interactive-graph', 'figure'),
     State('temperature-graph', 'figure'),
     State('scatter-3d-plot', 'figure'),
     State('pie-chart', 'figure'),
     State('ma-graph', 'figure'),
     State('lugeon-graph', 'figure'),
     State('injection-details', 'children'),
     State('mix-summary', 'children'),
     State('error-summary', 'children'),
     State('giv-operator-notes', 'children'),
     State('recipe-table', 'data'),
     State('qc-by-input', 'value')]
)
def print_report(n_clicks, figure, temp_figure, scatter_3d_figure, pie_figure, ma_figure, lugeon_figure, injection_details, mix_summary, error_summary, giv_operator_notes, recipe_table_data, qc_by):
    if n_clicks > 0:
        try:
            # Convert the figures to HTML divs
            plot_div = pio.to_html(figure, full_html=False)
            temp_plot_div = pio.to_html(temp_figure, full_html=False)
            scatter_3d_plot_div = pio.to_html(scatter_3d_figure, full_html=False)
            pie_chart_div = pio.to_html(pie_figure, full_html=False)
            ma_plot_div = pio.to_html(ma_figure, full_html=False)
            lugeon_plot_div = pio.to_html(lugeon_figure, full_html=False)

            # Convert recipe table to HTML
            recipe_table_html = """
            <table border="1">
                <tr>
                    <th>Recipe</th>
                    <th>Marsh</th>
                    <th>T. Start</th>
                    <th>T. Finish</th>
                    <th>Flow</th>
                    <th>Pe</th>
                    <th>Gauge</th>
                    <th>Gul</th>
                    <th>Volume</th>
                </tr>
            """
            for row in recipe_table_data:
                recipe_table_html += f"""
                <tr>
                    <td>{row['Recipe']}</td>
                    <td>{row['Marsh']}</td>
                    <td>{row['T. Start']}</td>
                    <td>{row['T. Finish']}</td>
                    <td>{row['Flow']}</td>
                    <td>{row['Pe']}</td>
                    <td>{row['Gauge']}</td>
                    <td>{row['Gul']}</td>
                    <td>{row['Volume']}</td>
                </tr>
                """
            recipe_table_html += "</table>"

            # Create the HTML content
            html_content = f"""
            <html>
                <head>
                    <title>Grout Gecko V-1.0 - QHBW Grouting Data QC Report</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        h1, h2, h3 {{ color: #333366; }}
                        .section {{ margin-bottom: 30px; }}
                        .details {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
                        .error {{ color: red; }}
                        table {{ border-collapse: collapse; width: 100%; }}
                        th, td {{ border: 1px solid black; padding: 8px; text-align: left; }}
                    </style>
                </head>
                <body>
                    <h1>Grout Gecko V-1.0 - QHBW Grouting Data QC Report</h1>
                    <div class="section">
                        <h2>Flow Rate and Effective Pressure Graph</h2>
                        {plot_div}
                    </div>
                    <div class="section">
                        <h2>Temperature Graph</h2>
                        {temp_plot_div}
                    </div>
                    <div class="section">
                        <h2>3D Scatter Plot</h2>
                        {scatter_3d_plot_div}
                    </div>
                    <div class="section">
                        <h2>Mix Volumes Pie Chart</h2>
                        {pie_chart_div}
                    </div>
                    <div class="section">
                        <h2>Noise Reduction (Moving Average) Graph</h2>
                        {ma_plot_div}
                    </div>
                    <div class="section">
                        <h2>Lugeon and Effective Pressure Graph</h2>
                        {lugeon_plot_div}
                    </div>
                    <div class="section">
                        <h2>Recipe Table</h2>
                        {recipe_table_html}
                    </div>
                    <div class="section">
                        <h2>Grout Injection Summary</h2>
                        <div class="details">{injection_details}</div>
                    </div>
                    <div class="section">
                        <h2>Mix Summary</h2>
                        <div class="details">{mix_summary}</div>
                    </div>
                    <div class="section">
                        <h2>Observations/Errors</h2>
                        <div class="details error">{error_summary}</div>
                    </div>
                    <div class="section">
                        <h2>GIV Operator Notes</h2>
                        <div class="details">{giv_operator_notes}</div>
                    </div>
                    <div class="section">
                        <h2>QC By</h2>
                        <div class="details">{qc_by or 'Not specified'}</div>
                    </div>
                </body>
            </html>
            """

            # Write the HTML content to a file
            output_path = 'Grout_Gecko_V1.0_QHBW_Grouting_Data_QC_Report.html'
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            # Open the HTML file in the default web browser
            import webbrowser
            webbrowser.open('file://' + os.path.realpath(output_path), new=2)

            print(f"HTML report generated and opened: {output_path}")

        except Exception as e:
            log_error(f"Error generating report: {e}")

    return 0

if __name__ == '__main__':
    app.run_server(debug=False)
