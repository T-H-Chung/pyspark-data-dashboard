from dash_extensions import WebSocket
from dash_extensions.enrich import html, dcc, Output, Input, DashProxy
import dash
import plotly.graph_objs as go
import json
import requests
import pandas as pd
import datetime
import psycopg2

# API Requests for news div
news_requests = requests.get(
    "https://newsapi.org/v2/top-headlines?sources=bbc-news&apiKey=da8e2e705b914f9f86ed2e9692e66012"
)

# API Call to update news
def update_news():
    json_data = news_requests.json()["articles"]
    df = pd.DataFrame(json_data)
    df = pd.DataFrame(df[["title", "url"]])
    max_rows = 10
    return html.Div(
        children=[
            html.P(className="p-news", children="Headlines"),
            html.P(
                className="p-news float-right",
                children="Last update : "
                + datetime.datetime.now().strftime("%H:%M:%S"),
            ),
            html.Table(
                className="table-news",
                children=[
                    html.Tr(
                        children=[
                            html.Td(
                                children=[
                                    html.A(
                                        className="td-link",
                                        children=df.iloc[i]["title"],
                                        href=df.iloc[i]["url"],
                                        target="_blank",
                                    )
                                ]
                            )
                        ]
                    )
                    for i in range(min(len(df), max_rows))
                ],
            ),
        ]
    )

def colored_bar_trace(df):
    return go.Ohlc(
        x=df.index,
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        showlegend=False,
        name="colored bar",
    )

# Create small example app.
app = DashProxy(__name__)
app.layout = html.Div(className="row", children=[
    WebSocket(id="ws", url="ws://localhost:8000/ws"),
    # Left Panel Div
    html.Div(
        className="three columns div-left-panel",
        children=[        
            # Interval component for news updates
            dcc.Interval(id="i_news", interval=1 * 60000, n_intervals=0),
            # Div for Left Panel App Info
            html.Div(
                className="div-info",
                children=[
                    html.A(
                        html.Img(
                            className="logo",
                            src=app.get_asset_url("Bitcoin_stream_data.jpg"),
                        ),
                        href="https://plotly.com/dash/",
                    ),
                    html.H6(className="title-header", children="FOREX TRADER"),
                    dcc.Markdown(
                        """
                        This app continually displays the stream data processed by Apache Kafka and Apache Spark.
                        
                        Tech Stack: Websocket, Apache Kafka, Apache Spark, Postgres, FastAPI, and Dash

                        [Source Code] |
                        [Layout Adapted from Dash Gallery](https://github.com/plotly/dash-sample-apps/tree/main/apps/dash-web-trader)
                        """
                    ),
                ],
            ),
            # Div for News Headlines
            html.Div(
                className="div-news",
                children=[html.Div(id="news", children=update_news())],
            ),
        ],
    ),
    # Right Panel Div
    html.Div(
        className="nine columns div-right-panel",
        children=[
            html.Div(
                className="row chart-top-bar",
                children=[
                    html.Span(
                        # id=pair + "menu_button",
                        className="inline-block chart-title",
                        children=f"BTC-USD",
                        n_clicks=0,
                    ),
                    # Dropdown and close button float right
                    html.Div(
                        className="graph-top-right inline-block",
                        children=[
                            html.Div(
                                className="inline-block",
                                children=[
                                    dcc.Dropdown(
                                        className="dropdown-period",
                                        # id=pair + "dropdown_period",
                                        options=[
                                            {"label": "5 min", "value": "5Min"},
                                            {"label": "15 min", "value": "15Min"},
                                            {"label": "30 min", "value": "30Min"},
                                        ],
                                        value="15Min",
                                        clearable=False,
                                    )
                                ],
                            ),
                        ],
                    ),
                ],
            ),  
            dcc.Graph(
                id='live-update-graph', className="chart-graph",
                config={"displayModeBar": True, "scrollZoom": True},
                figure={
                'data': [],
                'layout': go.Layout(paper_bgcolor="#21252C", 
                                    plot_bgcolor="#21252C")
            }),
            dcc.Interval(
                id='ratio-interval',
                interval=1*60000,  # in milliseconds
                n_intervals=0
            ),
            dcc.Graph(
                id='ratio-graph', className="chart-graph",
                config={"displayModeBar": True, "scrollZoom": True},
                figure={
                'data': [],
                'layout': go.Layout(paper_bgcolor="#21252C", 
                                    plot_bgcolor="#21252C")
            }),
        ],
    )
])


# Global variable to store data points
global_data = pd.DataFrame(columns=["product_id", "start_time", "end_time", "open", "low", "high", "close"])

@app.callback(
    Output("live-update-graph", "figure"), 
    [Input("ws", "message")], 
    prevent_initial_call=True
)
def update_graph_live(data):
    global global_data
    if data:
        try:
            data_dict = json.loads(data['data'])
            timestamp = data_dict['start_time']
            global_data.loc[timestamp] = data_dict
            # Limit the number of points to keep the graph manageable
            if len(global_data) > 36:
                global_data = global_data.iloc[-36:]
            # Construct the graph
            figure = {
                'data': [
                    go.Ohlc(
                        x=global_data.index,
                        open=global_data["open"],
                        high=global_data["high"],
                        low=global_data["low"],
                        close=global_data["close"],
                        showlegend=False,
                        name="colored bar",
                    )],
                'layout': go.Layout(paper_bgcolor="#21252C", 
                                    plot_bgcolor="#21252C")
            }
            
            figure["layout"][
                "uirevision"
            ] = "The User is always right"  # Ensures zoom on graph is the same on update
            figure["layout"]["margin"] = {"t": 50, "l": 50, "b": 50, "r": 25}
            figure["layout"]["autosize"] = True
            figure["layout"]["height"] = 400
            figure["layout"]["xaxis"]["rangeslider"]["visible"] = False
            figure["layout"]["xaxis"]["tickformat"] = "%H:%M"
            figure["layout"]["yaxis"]["showgrid"] = True
            figure["layout"]["yaxis"]["gridcolor"] = "#3E3F40"
            figure["layout"]["yaxis"]["gridwidth"] = 1
            return figure
        except ValueError:
            pass
    # Return the existing figure if no new data is available or in case of an error
    return dash.no_update

def fetch_data():
    """
    Fetch data from PostgreSQL database.
    """
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        host='localhost',
        port= 5432,
        dbname='btc',
        user='coin_user',
        password='coin_password'
    )
    sql_query = "SELECT * FROM btc_eth ORDER BY time DESC LIMIT 36"
    df = pd.read_sql_query(sql_query, conn)
    conn.close()
    return df

@app.callback(Output('ratio-graph', 'figure'),
              [Input('ratio-interval', 'n_intervals')])
def update_ratio(n):
    # Fetch new data
    df = fetch_data()
    # Create a Plotly figure
    fig = go.Figure()
    # Add a line plot trace
    fig.add_trace(go.Scatter(x=df['time'], y=df['ratio'],
                             mode='lines',  # This creates a line plot
                             line=dict(color='#1f77b4'),
                             name='Line Plot'))
    # Add a scatter plot trace
    fig.add_trace(go.Scatter(x=df['time'], y=df['ratio'],
                             mode='markers',  # This creates a scatter plot
                             marker=dict(color='#1f77b4'),
                             name='Scatter Plot'))
    # Update layout with the specified properties
    fig.update_layout(
        uirevision="The User is always right",  # Ensures zoom on graph is the same on update
        margin=dict(t=50, l=50, b=50, r=25),  # Set margins
        autosize=True,
        height=400,
        showlegend=False,
        xaxis=dict(
            rangeslider=dict(visible=False),
            gridcolor="#3E3F40",
            tickformat="%H:%M"
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="#3E3F40",
            gridwidth=1
        ),
        paper_bgcolor="#21252C", 
        plot_bgcolor="#21252C"
    )
    
    return fig

# Callback to update news
@app.callback(Output("news", "children"), [Input("i_news", "n_intervals")])
def update_news_div(n):
    return update_news()


if __name__ == "__main__":
    app.run_server(host='0.0.0.0', debug=True)