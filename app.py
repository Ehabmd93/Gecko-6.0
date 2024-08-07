import sys
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
from dash import Dash, dcc, html, Input, Output, State, dash_table
import plotly.graph_objects as go
import plotly.express as px
import base64
import io
import os
import webbrowser
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from dask.distributed import Client, LocalCluster
import logging

# Set up logging
logging.basicConfig(filename='app_errors.log', level=logging.ERROR, format='%(asctime)s %(levelname)s: %(message)s')

def log_error(message):
    print(message)  # Print to console
    logging.error(message)  # Log to file

# Environment variables
DATABASE_URL = os.environ.get('DATABASE_URL', 'your_database_url_here')

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

# Initialize Dask client
def initialize_dask_client():
    return Client(LocalCluster(processes=False, threads_per_worker=4, n_workers=2))

# Use this function to get the client when needed
def get_dask_client():
    global client
    if 'client' not in globals() or not client.scheduler_info():
        client = initialize_dask_client()
    return client

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
    html.Button('Bar Chart (Stage Intake)', id='show-stage-volumes-button', n_clicks=0, style={'marginTop': 20, 'marginBottom': 20}),
    
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
            dcc.Graph(id='stage-volumes-graph', style={'marginTop': 20, 'display': 'none'}),
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

    try:
        client = get_dask_client()
        
        if trigger_id == 'upload-data' and contents:
            df, mixes_and_marsh = parse_contents(contents, filename)
            if df is None:
                raise ValueError("Error parsing file contents")

            fig, temp_fig, scatter_3d_fig, pie_fig, data, notes_data, lugeon_fig = generate_interactive_graph(df)
            
            # Generate MA graph
            df_ma = apply_tma(df)
            ma_fig = go.Figure()
            add_trace(ma_fig, df_ma.compute(), 'Flow Rate (MA)', 'flow_tma', 'blue')
            add_trace(ma_fig, df_ma.compute(), 'Effective Pressure (MA)', 'effPressure_tma', 'green', yaxis='y2')
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
            
            stage_length = df['stageBottom'].compute().iloc[0] - df['stageTop'].compute().iloc[0]
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

        elif trigger_id == 'load-data-button' and hole_id and stage:
            print(f"Attempting to load data for hole_id: {hole_id}, stage: {stage}")
            data = retrieve_processed_data(hole_id, stage)
            if data is None or data.npartitions == 0:
                raise ValueError(f"No data found for Hole ID: {hole_id} and Stage: {stage}")

            print(f"Data retrieved successfully. Shape: {data.shape[0].compute()} rows")
            mixes_and_marsh = track_mixes_and_marsh_values(data)
            
            fig, temp_fig, scatter_3d_fig, pie_fig, _, notes_data, lugeon_fig = generate_interactive_graph(data)
            
            # Generate MA graph
            data_ma = apply_tma(data)
            ma_fig = go.Figure()
            add_trace(ma_fig, data_ma.compute(), 'Flow Rate (MA)', 'flow_tma', 'blue')
            add_trace(ma_fig, data_ma.compute(), 'Effective Pressure (MA)', 'effPressure_tma', 'green', yaxis='y2')
            ma_fig.update_layout(
                title='Noise Reduction (Moving Average)',
                yaxis=dict(title='Flow Rate (L/min)', side='left'),
                yaxis2=dict(title='Effective Pressure (bar)', overlaying='y', side='right'),
                hovermode='x unified'
            )
            
            injection_details = update_injection_details(data, stage, hole_id)
            mix_summary = html.Div([
                html.P(f"Mix {mix}: {count} times") for mix, count in mixes_and_marsh.items() if mix not in ['Cumulative Zero Flow', 'Marsh Changes']
            ])
            
            stage_length = data['stageBottom'].compute().iloc[0] - data['stageTop'].compute().iloc[0]
            error_summary = []
            if stage_length < 6:
                error_summary.append("Short stage length detected")
            elif stage_length > 6:
                error_summary.append("Long stage length detected")
            error_summary.append(f"Marsh changes: {mixes_and_marsh['Marsh Changes']}")
            error_summary.append(f"Cumulative Zero Flow: {mixes_and_marsh['Cumulative Zero Flow']:.2f} hours")
            error_summary = html.Div([html.P(error) for error in error_summary])

            giv_operator_notes = html.Div([
                html.H3("GIV Operator Notes:"),
                html.Pre(update_notes_table(notes_data))
            ]) if not notes_data.empty else ""

            return f"Data for {hole_id} {stage} loaded successfully", "", fig, temp_fig, scatter_3d_fig, pie_fig, ma_fig, lugeon_fig, injection_details, mix_summary, error_summary, giv_operator_notes, ""

    except Exception as e:
        error_message = f"Error processing data: {str(e)}"
        log_error(error_message)
        return "", error_message, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, ""

    raise PreventUpdate

# Callback to show stage volumes
@app.callback(
    Output('stage-volumes-graph', 'style'),
    Output('stage-volumes-graph', 'figure'),
    Input('show-stage-volumes-button', 'n_clicks'),
    State('hole-id-dropdown', 'value'),
    State('stage-dropdown', 'value')
)
def show_stage_volumes(n_clicks, hole_id, selected_stage):
    if n_clicks is None or not hole_id or not selected_stage:
        raise PreventUpdate
    
    df = calculate_stage_volumes()
    if df.empty:
        return {'display': 'none'}, {}

    selected_stage_volume = df.loc[(df['stage'] == selected_stage) & (df['hole_id'] == hole_id), 'total_volume'].values[0]
    left_stages = df[df['total_volume'] < selected_stage_volume]
    right_stages = df[df['total_volume'] > selected_stage_volume]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=left_stages['stage_hole_id'],
        y=left_stages['total_volume'],
        name='Less Grout Intake',
        marker_color='rgba(222,45,38,0.8)'
    ))

    fig.add_trace(go.Bar(
        x=[f"{selected_stage}, {hole_id}"],
        y=[selected_stage_volume],
        name='Selected Stage',
        marker_color='rgba(55,128,191,0.8)'
    ))

    fig.add_trace(go.Bar(
        x=right_stages['stage_hole_id'],
        y=right_stages['total_volume'],
        name='More Grout Intake',
        marker_color='rgba(50,171,96,0.8)'
    ))

    fig.update_layout(
        title='Stage Total Grout Volume Intake',
        xaxis_title='Stage and Hole ID',
        yaxis_title='Total Volume (L)',
        barmode='stack'
    )

    return {'display': 'block' if n_clicks % 2 == 1 else 'none'}, fig

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
            webbrowser.open('file://' + os.path.realpath(output_path), new=2)

            print(f"HTML report generated and opened: {output_path}")

        except Exception as e:
            log_error(f"Error generating report: {e}")

    return 0

if __name__ == '__main__':
    client = initialize_dask_client()
    app.run_server(debug=False)
