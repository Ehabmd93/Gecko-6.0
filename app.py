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
engine = create_engine(DATABASE_URL, poolclass=QueuePool, pool_size=10, max_overflow=20)
Session = sessionmaker(bind=engine)
session = Session()

# Define metadata
metadata = MetaData()

# Define database tables
processed_data = Table(
    'processed_data', metadata,
    Column('id', Integer, primary_key=True),
    Column('hole_id', String),
    Column('stage', String),
    Column('TIMESTAMP', TIMESTAMP),
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

grouting_summary = Table(
    'grouting_summary', metadata,
    Column('id', Integer, primary_key=True),
    Column('hole_id', String),
    Column('stage', String),
    Column('start_time', TIMESTAMP),
    Column('end_time', TIMESTAMP),
    Column('total_volume', Float),
    Column('mix_a_volume', Float),
    Column('mix_b_volume', Float),
    Column('mix_c_volume', Float),
    Column('mix_d_volume', Float),
    Column('total_grouting_time', Float),
    Column('net_grouting_time', Float)
)

available_data = Table(
    'available_data', metadata,
    Column('id', Integer, primary_key=True),
    Column('hole_id', String),
    Column('stage', String),
    Column('upload_date', TIMESTAMP)
)

# Create tables if they don't exist
metadata.create_all(engine)

def test_database_connection():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            print("Database connection successful!")
    except Exception as e:
        print(f"Database connection failed: {e}")

# Call this function at the start of your app
test_database_connection()

# Initialize Dash app
app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server

# Function to get unique values from the database
def get_unique_values(table_name, column_name, hole_id=None):
    try:
        with engine.connect() as connection:
            if column_name == 'hole_id':
                query = text("SELECT DISTINCT hole_id FROM processed_data")
                result = connection.execute(query)
                return [row[0] for row in result]
            elif column_name == 'stage':
                if hole_id:
                    query = text("SELECT DISTINCT stage FROM processed_data WHERE hole_id = :hole_id")
                    result = connection.execute(query, {"hole_id": hole_id})
                    return [row[0] for row in result]
                else:
                    return []
    except Exception as e:
        print(f"Error getting unique values: {e}")
        return []

# Function to extract file details
def extract_file_details(file_name):
    try:
        pattern = r'([PS]\d{4})_(S\d+(?:&\d+)?)'
        match = re.search(pattern, file_name)
        if match:
            hole_id, stage = match.groups()
            order = 'P' if hole_id.startswith('P') else 'S'
            return hole_id, stage, order
    except Exception as e:
        print(f"Error extracting file details from {file_name}: {e}")
    return None, None, None

# Function to clean data
def clean_data(data):
    try:
        headers = [
            'TIMESTAMP', 'RECORD', 'CumTimeMin', 'flow', 'AvgFlow', 'volume', 'gaugePressure',
            'AvgGaugePressure', 'effPressure', 'AvgEffectivePressure', 'Lugeon', 'Batt_V', 'PTemp', 
            'holeNum', 'mixNum', 'waterTable', 'stageTop', 'stageBottom', 'gaugeHeight', 
            'waterDepth', 'holeAngle', 'vmarshGrout', 'vmarshWater', 'Notes'
        ]
        
        data = data.drop([0, 1, 2])
        data.columns = headers
        data = data.drop(data.index[0])
        data = data.reset_index(drop=True)

        data['TIMESTAMP'] = pd.to_datetime(data['TIMESTAMP'], errors='coerce')
        data = data.dropna(subset=['TIMESTAMP'])

        numeric_columns = ['flow', 'AvgFlow', 'volume', 'gaugePressure', 'AvgGaugePressure', 
                           'effPressure', 'AvgEffectivePressure', 'Lugeon', 'Batt_V', 'PTemp', 
                           'holeNum', 'mixNum', 'waterTable', 'stageTop', 'stageBottom', 
                           'gaugeHeight', 'waterDepth', 'holeAngle', 'vmarshGrout', 'vmarshWater']
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')

        return data
    except Exception as e:
        print(f"Error cleaning data: {e}")
        return None

# Function to parse file contents
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
        print(f"Error parsing file: {e}")
        return None, None

# Function to store processed data into the database
def store_processed_data(df, hole_id, stage):
    try:
        records = df.to_dict(orient='records')
        for record in records:
            record['hole_id'] = hole_id
            record['stage'] = stage
            insert_stmt = processed_data.insert().values(**record)
            session.execute(insert_stmt)
        session.commit()
        print(f"Data for {hole_id} {stage} stored successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error storing processed data: {e}")

# Function to store available data into the database
def store_available_data(hole_id, stage):
    try:
        insert_stmt = available_data.insert().values(hole_id=hole_id, stage=stage, upload_date=dt.datetime.now())
        session.execute(insert_stmt)
        session.commit()
        print(f"Available data for {hole_id} {stage} stored successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error storing available data: {e}")

# Function to retrieve processed data from the database
def retrieve_processed_data(hole_id, stage):
    try:
        with engine.connect() as connection:
            query = text("""
                SELECT * FROM processed_data 
                WHERE hole_id = :hole_id AND stage = :stage
                LIMIT 10000
            """)
            result = connection.execute(query, {"hole_id": hole_id, "stage": stage})
            df = pd.DataFrame(result.fetchall())
            if df.empty:
                print(f"No data found for hole_id: {hole_id} and stage: {stage}")
            else:
                print(f"Retrieved {len(df)} rows for hole_id: {hole_id} and stage: {stage}")
            return df
    except Exception as e:
        print(f"Error retrieving processed data: {e}")
        return None

# Function to track mixes and marsh values
def track_mixes_and_marsh_values(data):
    mix_counts = {'A': 0, 'B': 0, 'C': 0, 'D': 0}
    marsh_values = {'A': None, 'B': None, 'C': None, 'D': None}
    error_list = []
    current_mix = 'A'

    for i, row in data.iterrows():
        timestamp = row['TIMESTAMP']
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

    total_grouting_time = (data['TIMESTAMP'].iloc[-1] - data['TIMESTAMP'].iloc[0]).total_seconds() / 3600
    zero_flow_interval = calculate_cumulative_zero_flow(data)
    net_grouting_time = total_grouting_time - zero_flow_interval

    return {
        'Mix A': mix_counts['A'],
        'Mix B': mix_counts['B'],
        'Mix C': mix_counts['C'],
        'Mix D': mix_counts['D'],
        'Total Grouting Time': total_grouting_time,
        'Net Grouting Time': net_grouting_time,
        'Cumulative Zero Flow': zero_flow_interval,
        'Errors': error_list
    }

# Function to identify marsh changes
def identify_marsh_changes(data):
    try:
        marsh_changes = data[data['vmarshGrout'].diff() != 0]
        marsh_changes = marsh_changes[['TIMESTAMP', 'mixNum', 'vmarshGrout', 'flow']]
        return marsh_changes
    except Exception as e:
        print(f"Error identifying Marsh changes: {e}")
        return pd.DataFrame(columns=['TIMESTAMP', 'mixNum', 'vmarshGrout', 'flow'])

# Function to extract notes
def extract_notes(data):
    try:
        if 'Notes' not in data.columns:
            return pd.DataFrame(columns=['Timestamp', 'Note'])

        notes_data = data[['TIMESTAMP', 'Notes']].dropna(subset=['Notes'])
        if notes_data.empty:
            return pd.DataFrame(columns=['Timestamp', 'Note'])

        notes_data.columns = ['Timestamp', 'Note']
        notes_data['Timestamp'] = pd.to_datetime(notes_data['Timestamp']).dt.strftime('%H:%M')
        return notes_data
    except Exception as e:
        print(f"Error extracting notes: {e}")
        return pd.DataFrame(columns=['Timestamp', 'Note'])

# Function to generate interactive graph
def generate_interactive_graph(data):
    try:
        if data is None or data.empty:
            return None, None, None

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

        return fig, data, notes_data
    except Exception as e:
        print(f"Error generating interactive graph: {e}")
        return None, None, None

# Helper function to add trace to figure
def add_trace(fig, data, name, y_col, color, yaxis='y'):
    fig.add_trace(go.Scatter(
        x=data['ElapsedMinutes'],
        y=data[y_col],
        mode='lines+markers',
        name=name,
        line=dict(color=color),
        text=[f"Time: {row['TIMESTAMP'].strftime('%H:%M')}, Flow: {row['flow']:.1f}, Eff Pressure: {row['effPressure']:.1f}, Gauge Pressure: {row['gaugePressure']:.1f}, Lugeon: {row['Lugeon']:.1f}, Volume: {row['volume']:.1f}" 
              for _, row in data.iterrows()],
        hoverinfo='text',
        yaxis=yaxis
    ))

# Function to generate scatter plot
def generate_scatter_plot(data):
    try:
        fig = px.scatter(data, x='flow', y='effPressure', color='mixNum', 
                         labels={'flow': 'Flow Rate', 'effPressure': 'Effective Pressure', 'mixNum': 'Mix Number'},
                         title='Flow Rate vs Effective Pressure',
                         category_orders={'mixNum': [2, 3, 4, 5]},
                         color_discrete_map={2: 'blue', 3: 'cyan', 4: 'magenta', 5: 'orange'})
        return fig
    except Exception as e:
        print(f"Error generating scatter plot: {e}")
        return None

# Function to generate histogram
def generate_histogram(data, column):
    try:
        fig = px.histogram(data, x=column, nbins=20, title=f'Distribution of {column}')
        return fig
    except Exception as e:
        print(f"Error generating histogram: {e}")
        return None

# Function to calculate cumulative zero flow
def calculate_cumulative_zero_flow(data):
    try:
        zero_flow_intervals = []
        current_interval = None
        for i, row in data.iterrows():
            if row['flow'] == 0:
                if current_interval is None:
                    current_interval = {'start': row['TIMESTAMP'], 'end': row['TIMESTAMP']}
                else:
                    current_interval['end'] = row['TIMESTAMP']
            else:
                if current_interval is not None and (row['TIMESTAMP'] - current_interval['end']).total_seconds() > 180:
                    interval_duration = (current_interval['end'] - current_interval['start']).total_seconds() / 60
                    if interval_duration > 3:  # Only consider intervals longer than 3 minutes
                        zero_flow_intervals.append(interval_duration)
                    current_interval = None
        cumulative_zero_flow = sum(zero_flow_intervals) / 60  # in hours
        return cumulative_zero_flow
    except Exception as e:
        print(f"Error calculating cumulative zero flow: {e}")
        return 0

# Function to calculate grout and mix volumes
def calculate_grout_and_mix_volumes(data):
    try:
        cumulative_grout = data['volume'].iloc[-1]
        mix_a_volume = mix_b_volume = mix_c_volume = mix_d_volume = 0

        if not data.empty:
            mix_changes = data[data['mixNum'].diff() != 0]
            
            # Calculate mix A volume
            mix_a_end = mix_changes[mix_changes['mixNum'] == 3]
            mix_a_volume = mix_a_end['volume'].iloc[0] if not mix_a_end.empty else cumulative_grout
            
            # Calculate mix B volume
            mix_b_end = mix_changes[mix_changes['mixNum'] == 4]
            mix_b_volume = mix_b_end['volume'].iloc[0] - mix_a_volume if not mix_b_end.empty else (cumulative_grout - mix_a_volume if data['mixNum'].max() >= 3 else 0)
            
            # Calculate mix C volume
            mix_c_end = mix_changes[mix_changes['mixNum'] == 5]
            mix_c_volume = mix_c_end['volume'].iloc[0] - (mix_a_volume + mix_b_volume) if not mix_c_end.empty else (cumulative_grout - mix_a_volume - mix_b_volume if data['mixNum'].max() >= 4 else 0)
            
            # Calculate mix D volume
            mix_d_volume = cumulative_grout - (mix_a_volume + mix_b_volume + mix_c_volume) if data['mixNum'].max() == 5 else 0

        return mix_a_volume, mix_b_volume, mix_c_volume, mix_d_volume, cumulative_grout
    except Exception as e:
        print(f"Error calculating grout and mix volumes: {e}")
        return 0, 0, 0, 0, 0

# Function to update injection details
def update_injection_details(data, selected_stage, selected_hole_id):
    try:
        non_zero_flow = data[data['flow'] > 0]
        first_non_zero_flow_time = non_zero_flow['TIMESTAMP'].min()
        last_timestamp = data['TIMESTAMP'].max()

        stage_top = data.loc[(data['TIMESTAMP'] - first_non_zero_flow_time).abs().idxmin() + 10, 'stageTop']
        stage_bottom = data.loc[(data['TIMESTAMP'] - first_non_zero_flow_time).abs().idxmin() + 10, 'stageBottom']

        first_date = first_non_zero_flow_time.strftime('%Y-%m-%d')
        first_time = first_non_zero_flow_time.strftime('%H:%M:%S')
        last_date = last_timestamp.strftime('%Y-%m-%d')
        last_time = last_timestamp.strftime('%H:%M:%S')

        mix_a_volume, mix_b_volume, mix_c_volume, mix_d_volume, cumulative_volume = calculate_grout_and_mix_volumes(data)

        total_grouting_time = (last_timestamp - first_non_zero_flow_time).total_seconds() / 3600
        zero_flow_interval = calculate_cumulative_zero_flow(data)
        net_grouting_time = total_grouting_time - zero_flow_interval

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
            html.H3("Net Mixes Volumes:"),
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
        ]

        return html.Div(details)
    except Exception as e:
        print(f"Error updating injection details: {e}")
        return "Error updating injection details"

# Function to update notes table
def update_notes_table(notes_data):
    try:
        if notes_data is None or notes_data.empty:
            return "No notes available"
        comments = [f"{row['Timestamp']} - {row['Note']}" for _, row in notes_data.iterrows()]
        return "\n".join(comments)
    except Exception as e:
        print(f"Error updating notes table: {e}")
        return "Error updating notes table"

# Load and encode the Gecko logo
encoded_logo = None
try:
    with open('Lizard_Logo.jpg', 'rb') as f:
        encoded_logo = base64.b64encode(f.read()).decode('ascii')
except FileNotFoundError:
    print("Logo file 'Lizard_Logo.jpg' not found. The app will run without the logo.")

# Define the layout for the app
app.layout = html.Div([
    html.Div([
        html.Div([
            html.H1("Gecko 1.0", style={'fontSize': '48px', 'fontWeight': 'bold', 'color': 'black', 'margin': '0'}),
            html.H3("QHBW Grouting Data QC Tool and Plotter", style={'margin': '0'})
        ], style={'flex': '1'}),
        html.Img(src=f'data:image/jpg;base64,{encoded_logo}', style={'height': '100px', 'width': 'auto'}) if encoded_logo else html.Div()
    ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'space-between', 'marginBottom': '20px'}),
     
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
    
    html.Div([
        html.Label('View Type'),
        dcc.RadioItems(
            id='view-type',
            options=[
                {'label': 'Time Series', 'value': 'time_series'},
                {'label': 'Scatter Plot', 'value': 'scatter'},
                {'label': 'Histogram', 'value': 'histogram'}
            ],
            value='time_series',
            labelStyle={'display': 'inline-block', 'marginRight': '10px'}
        )
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
    
    html.Button('Run Tool', id='run-tool-button-1', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20}),
    html.Button('Load Data', id='load-data-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}),
    html.Button('Upload to Database', id='upload-db-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20, 'marginLeft': 10}),
    
    dcc.Loading(
        id="loading",
        type="default",
        children=[
            html.Div(id='processing-message', style={'textAlign': 'center', 'fontSize': '18px', 'marginTop': '20px'}),
            dcc.Graph(id='interactive-graph'),
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
        ], style={'width': '50%', 'display': 'inline-block', 'verticalAlign': 'top'}),
        html.Div([
            html.Div(id='injection-details')
        ], style={'width': '50%', 'display': 'inline-block', 'verticalAlign': 'top', 'paddingLeft': '20px'})
    ], style={'display': 'flex', 'marginTop': '20px'}),
    
    html.Div([
        html.H3("Mixes Count"),
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
    flow = text_parts[1].split(': ')[1]
    eff_pressure = text_parts[2].split(': ')[1]
    gauge_pressure = text_parts[3].split(': ')[1]
    lugeon = text_parts[4].split(': ')[1]
    
    return f"Flow: {flow} LPM, Effective Pressure: {eff_pressure} Bar, Gauge Pressure: {gauge_pressure} Bar, Lugeon: {lugeon}, Timestamp: {timestamp}"

# Main callback to update graph and details
@app.callback(
    [Output('upload-confirmation', 'children'),
     Output('error-message', 'children'),
     Output('interactive-graph', 'figure'),
     Output('injection-details', 'children'),
     Output('mix-summary', 'children'),
     Output('error-summary', 'children'),
     Output('giv-operator-notes', 'children'),
     Output('processing-message', 'children')],
    [Input('upload-data', 'contents'),
     Input('run-tool-button-1', 'n_clicks'),
     Input('load-data-button', 'n_clicks'),
     Input('hole-id-dropdown', 'value'),
     Input('stage-dropdown', 'value'),
     Input('view-type', 'value')],
    [State('upload-data', 'filename')]
)
def update_and_run_tool(contents, run_clicks, load_clicks, hole_id, stage, view_type, filename):
    ctx = dash.callback_context
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger_id == 'upload-data' and contents:
        try:
            df, mixes_and_marsh = parse_contents(contents, filename)
            if df is None:
                raise ValueError("Error parsing file contents")

            fig, data, notes_data = generate_interactive_graph(df)
            injection_details = update_injection_details(df, stage, hole_id)
            mix_summary = html.Div([
                html.P(f"Mix {mix}: {count} times") for mix, count in mixes_and_marsh.items() if mix != 'Errors'
            ])
            error_summary = html.Div([html.Span(error, style={'color': 'red'}) for error in mixes_and_marsh['Errors']] or "NA")
            giv_operator_notes = html.Div([
                html.H3("GIV Operator Notes:"),
                html.Pre(update_notes_table(notes_data))
            ])

            return (f"File '{filename}' processed successfully", "",
                    fig, injection_details, mix_summary, error_summary, giv_operator_notes, "")
        except Exception as e:
            error_message = f"Error processing file: {str(e)}"
            print(error_message)
            return "", error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""

    elif trigger_id == 'load-data-button' and hole_id and stage:
        try:
            print(f"Attempting to load data for hole_id: {hole_id}, stage: {stage}")
            data = retrieve_processed_data(hole_id, stage)
            if data is None or data.empty:
                error_message = f"No data found for Hole ID: {hole_id} and Stage: {stage}"
                print(error_message)
                return "", error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""
            
            print(f"Data retrieved successfully. Shape: {data.shape}")
            mixes_and_marsh = track_mixes_and_marsh_values(data)
            
            if view_type == 'time_series':
                fig, _, notes_data = generate_interactive_graph(data)
            elif view_type == 'scatter':
                fig = generate_scatter_plot(data)
            elif view_type == 'histogram':
                fig = generate_histogram(data, 'flow')  # You can change 'flow' to any other column
            else:
                fig = go.Figure()  # Empty figure if view type is not recognized
            
            injection_details = update_injection_details(data, stage, hole_id)
            mix_summary = html.Div([
                html.P(f"Mix {mix}: {count} times") for mix, count in mixes_and_marsh.items() if mix != 'Errors'
            ])
            error_summary = html.Div([html.Span(error, style={'color': 'red'}) for error in mixes_and_marsh['Errors']] or "NA")
            giv_operator_notes = html.Div([
                html.H3("GIV Operator Notes:"),
                html.Pre(update_notes_table(notes_data))
            ])

            return f"Data for {hole_id} {stage} loaded successfully", "", fig, injection_details, mix_summary, error_summary, giv_operator_notes, ""
        except Exception as e:
            error_message = f"Error loading data: {str(e)}"
            print(error_message)
            return "", error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""

    raise PreventUpdate

# Callback for uploading data to the database
@app.callback(
    Output('upload-db-button', 'children'),
    [Input('upload-db-button', 'n_clicks')],
    [State('upload-data', 'contents'),
     State('upload-data', 'filename')]
)
def upload_to_database(n_clicks, contents, filename):
    if n_clicks > 0 and contents:
        try:
            df, _ = parse_contents(contents, filename)
            hole_id, stage, _ = extract_file_details(filename)
            store_processed_data(df, hole_id, stage)
            store_available_data(hole_id, stage)
            return f"Data uploaded for {hole_id} {stage}"
        except Exception as e:
            print(f"Error uploading to database: {e}")
            return "Error uploading data"
    return "Upload to Database"

# Callback for printing the report
@app.callback(
    Output('print-report-button', 'n_clicks'),
    Input('print-report-button', 'n_clicks'),
    [State('interactive-graph', 'figure'),
     State('injection-details', 'children'),
     State('mix-summary', 'children'),
     State('error-summary', 'children'),
     State('giv-operator-notes', 'children'),
     State('recipe-table', 'data'),
     State('qc-by-input', 'value')]
)
def print_report(n_clicks, figure, injection_details, mix_summary, error_summary, giv_operator_notes, recipe_table_data, qc_by):
    if n_clicks > 0:
        try:
            # Convert the figure to an HTML div
            plot_div = pio.to_html(figure, full_html=False)

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
                    <title>Gecko 1.0 - QHBW Grouting Data QC Tool Report</title>
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
                    <h1>Gecko 1.0 - QHBW Grouting Data QC Tool Report</h1>
                    <div class="section">
                        <h2>Interactive Graph</h2>
                        {plot_div}
                    </div>
                    <div class="section">
                        <h2>Injection Details</h2>
                        <div class="details">{injection_details}</div>
                    </div>
                    <div class="section">
                        <h2>Mix Summary</h2>
                        <div class="details">{mix_summary}</div>
                    </div>
                    <div class="section">
                        <h2>Recipe Table</h2>
                        {recipe_table_html}
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
            output_path = 'Gecko_1.0_QHBW_Grouting_Data_QC_Report.html'
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            # Open the HTML file in the default web browser
            import webbrowser
            webbrowser.open('file://' + os.path.realpath(output_path), new=2)

            print(f"HTML report generated and opened: {output_path}")

        except Exception as e:
            print(f"Error generating report: {e}")

    return 0

if __name__ == '__main__':
    app.run_server(debug=False)
