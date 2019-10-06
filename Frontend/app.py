import ast
import math
import dash
import psycopg2
import dash_core_components as dcc
import dash_html_components as html
from collections import Counter
from dash.exceptions import PreventUpdate
from dash.dependencies import Input, Output, State, ClientsideFunction

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server

conn = psycopg2.connect("dbname=tutorial user=postgres")

global_data = {}

global_data["trending"] = {
    "0": ["stock",0,100],
    "1": ["stock",0,100],
    "2": ["stock",0,100],
    "3": ["stock",0,100],
    "4": ["stock",0,100],
}

def updateData():
    global global_data
    cur = conn.cursor()
    cur.execute("SELECT msg FROM intervalData ORDER BY id DESC LIMIT 1;")
    content = cur.fetchone()[0]
    content = content.split("\n")
    for i in content:
        if i == "":
            continue
        s = i.split("@")
        global_data[s[0]] = ast.literal_eval(s[1])
    return 

def get_color(arg):
    if arg == 100:
        return "black"
    elif arg > 100:
        return "#45b07e"
    else:
        return "#da5657"

def update_row():
    rows = []
    trending_list = global_data["trending"]
    while len(trending_list) < 5:
        trending_list[str(len(trending_list))] = ["stock",0,100]
    for index in trending_list:
        p = trending_list[index][2]-100
        row = html.Div(
            children=
            [
                html.Div(
                    id=index + "row",
                    className="row",
                    children=
                    [
                        html.P(trending_list[index][0], 
                        id=index + "symbol", 
                        className="trend-col", 
                        style={'display': 'inline-block','width': '33%'}),
                        html.P(trending_list[index][1],
                        id=index + "volume", 
                        className="trend-col",
                        style={'display': 'inline-block','width': '33%'}),
                        html.P(str(round(p,2))+" %",
                        id=index + "percent", 
                        className="trend-col",
                        style={'display': 'inline-block','width': '33%',"color": get_color(trending_list[index][2])})
                    ]
                )
            ]
        )
        rows.append(row)
    return rows

def update_info():
    if "volume" in global_data:
        volume = global_data["volume"]
    else:
        volume = 0
    if "delayed" in global_data:
        delay = global_data["delayed"]
    else:
        delay = {"stock":100}
    symbol = list(delay)[0]
    price = delay[symbol]
    cols = [
        html.Div(
            [html.H6("Total Volume"), html.P(str(volume))],
            id="volume_info",
            className="mini_container",
            style={'flex': 1},
        ),
        html.Div(
            [html.H6("Delayed Message"), html.P(symbol+": "+str(price))],
            id="delay_info",
            className="mini_container",
            style={'flex': 1},
        ),
        html.Div(
            [html.H6("Working Nodes"), html.P(3*math.ceil(volume/10000))],
            id="node_info",
            className="mini_container",
            style={'flex': 1},
        ),
        html.Div(
            [html.H6("System Status"), html.P("OK")],
            id="system_info",
            className="mini_container",
            style={'flex': 1},
        ),        
    ]
    return cols

def update_stream():
    updateData()
    return [update_row(),update_info()]

app.layout = html.Div(
    [
        dcc.Store(id='memory'),
        html.Div(id="output-clientside"),
        html.Div(
            [
                html.Div(
                    [
                        html.Img(
                            src=app.get_asset_url("dash-logo.png"),
                            id="plotly-image",
                            style=
                            {
                                "height": "60px",
                                "width": "auto",
                                "margin-bottom": "25px",
                            },
                        )
                    ],
                    className="one-third column",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3(
                                    "Elastic Trends Explorer",
                                    style={"margin-bottom": "5px"},
                                ),
                                # html.H5(
                                #     "Volatility Overview", style={"margin-top": "0px"},
                                # ),
                            ]
                        )
                    ],
                    id="title",
                    className="one-half column",
                ),
            ],
            id="header",
            className="row flex-display",
            style={"margin-bottom": "25px"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Div(
                            children=
                            [
                                dcc.Interval(id="interval", interval=5 * 1000, n_intervals=0),
                                html.Div(
                                children=
                                [
                                    html.H6(className="title-header", children="Current Trending Stocks"),
                                ]
                                )
                            ],
                            className="control_label"
                        ),
                        html.Div(
                            children = [
                                html.P(className="trend-col",children="Symbol",style={'display': 'inline-block','width': '33%'}),
                                html.P(className="trend-col",children="Volume",style={'display': 'inline-block','width': '33%'}),
                                html.P(className="trend-col",children="Percentage",style={'display': 'inline-block','width': '33%'}),
                                html.Div(
                                    id="trending-data",
                                    className="trending-data-class",
                                    children=update_row()
                                ),
                            ],
                            className="control_label"
                        ),
                        html.P("Choose Date Range:", className="control_label"),
                        dcc.DatePickerRange(
                            id='date-picker-range',
                            start_date_placeholder_text='Start date',
                            end_date_placeholder_text='End date',
                            style={'border': 0},
                            className="dcc_control",
                        ),
                        html.P("Filter by industry:", className="control_label"),
                        dcc.Dropdown(
                            id="industry",
                            options=[{'label': 'Technology', 'value': 'NYC'}],
                            #multi=True,
                            value="NYC",
                            className="dcc_control",
                        ),
                        html.P("Select Date Range",className="control_label",),
                        dcc.RangeSlider(
                            id="year_slider",
                            min=0,
                            max=100,
                            value=[0, 100],
                            className="dcc_control",
                        ),
                    ],
                    id="cross-filter-options",
                    className="pretty_container four columns",
                ),
                html.Div(
                    children=
                    [
                        html.Div(
                            children=update_info(),
                            id="info-container",
                            className="row container-display",
                        ),
                        html.Div(
                            [dcc.Graph(id="count_graph")],
                            id="countGraphContainer",
                            className="pretty_container",
                        ),
                        html.Br(),
                        html.P("Top Stock: ",id="top_stocks",className="pretty_container",)
                    ],
                    id="right-column",
                    className="eight columns",
                ),
            ],
            className="row flex-display",
        ),
    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)

app.clientside_callback(
    ClientsideFunction(namespace="clientside", function_name="resize"),
    Output("output-clientside", "children"),
    [Input("count_graph", "figure")],
)

layout = dict(
    autosize=True,
    automargin=True,
    showlegend=False,
    margin=dict(l=30, r=30, b=20, t=40),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
    title="Historical Data", 
    dragmode="select"
)

@app.callback(
    [
        Output('count_graph', 'figure'),
        Output('memory', 'data'),
    ],
    [
        Input("industry", "value"),
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
    ]
)
def make_count_figure(industry,start,end):
    result=[i for i in range(100)]
    if industry==None or start == None or end == None:
        c=["rgb(123, 199, 255)" for i in result]
        data = [
            dict(
                type="bar",
                x=result,
                y=result,
                name="Volatility",
                marker=dict(color=c),
            ),
        ]
        figure = dict(data=data, layout=layout)
        raise PreventUpdate
    cur = conn.cursor()
    industry = "energy"
    query = "SELECT * FROM %s WHERE time BETWEEN %%s::timestamp AND %%s::timestamp;" % industry
    cur.execute(query,(start,end,))
    result = cur.fetchall()
    if len(result) > 500:
        pass #sampling data
    percent = [i[1]-100 for i in result]
    datelist = [str(i[0])[:10] for i in result]
    # import pandas as pd
    # datelist = pd.to_datetime(datelist)
    # df = pd.DataFrame()
    # df["percent"] = percent
    # df = df.set_index(datelist)
    # df.index.name = "data"
    c=["rgb(123, 199, 255)" for i in result]
    data = [
        dict(
            type="bar",
            x=datelist,
            y=percent,
            name="Volatility",
            marker=dict(color=c),
        ),
    ]
    figure = dict(data=data, layout=layout)
    query = "SELECT * FROM %s_max WHERE time BETWEEN %%s::timestamp AND %%s::timestamp;" % industry
    cur.execute(query,(start,end,))
    result = cur.fetchall()

    return [figure,result]


@app.callback(
    Output("top_stocks", "children"),
    [Input("year_slider", "value")],
    [State("memory", "data")]
)
def calculate_top(val,memory):
    if memory == None:
        raise PreventUpdate
    l = len(memory)
    s = math.floor(l*val[0]/100)
    e = math.floor(l*val[1]/100)
    filterData = memory[s:e]
    filterData = [i[2] for i in filterData]
    c = Counter(filterData)
    sortCounter = [(c[i],i) for i in c]
    sortCounter.sort(key=lambda x:x[0],reverse=True)
    idx = min(5,len(sortCounter))
    msg = "Top Stock: "
    for i in sortCounter[:idx]:
        msg += i[1] + " " + str(i[0]) + " times. "
    return msg

@app.callback(
    [
        Output("trending-data", "children"),
        Output("info-container", "children"),
    ],
    [Input("interval", "n_intervals")])
def update_data(n):
    return update_stream()

update_stream()

if __name__ == "__main__":
    app.run_server(debug=True)