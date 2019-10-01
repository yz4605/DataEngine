from kafka import KafkaConsumer
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State, ClientsideFunction
import ast


app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server

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

consumer = KafkaConsumer('trend',group_id='group-trend',bootstrap_servers=['localhost:9092'])

trending_list = {
    "0": ["A",1,10],
    "1": ["B",2,20],
    "2": ["C",3,30],
    "3": ["D",4,40],
    "4": ["E",5,50],
}

flag = -1

def updateData():
    global trending_list
    # for i in trending_list:
    #     trending_list[i][1] += 1
    #     trending_list[i][2] += 10
    global flag
    flag += 1
    if flag < 1:
        return
    msg = consumer.poll(10)
    print("poll")
    if len(msg) == 0:
        return 
    content = msg[list(msg)[0]]
    trendsList = []
    for i in content:
        s = i.value.decode()
        trends = s.split(";")
        for t in trends:
            if t == '':
                continue
            symbolPrice = t.split(",",1)
            l = ast.literal_eval(symbolPrice[1])
            l.append(symbolPrice[0])
        trendsList.append(l)
    trendsList.sort(key=lambda x:x[0],reverse=True)
    filterTrends = set()
    newTrends = {}
    counter = 0
    for i in range(len(trendsList)):
        if trendsList[i][-1] in filterTrends:
            continue
        filterTrends.add(trendsList[i][-1])
        newTrends[str(counter)] = [trendsList[i][-1],trendsList[i][0],trendsList[i][-2]]
        counter += 1
    length = min(len(newTrends),len(trending_list))
    for i in range(length):
        trending_list[str(i)] = newTrends[str(i)]
    print(newTrends)
    return 


def get_color(arg):
    if arg == 100:
        return "black"
    elif arg > 100:
        return "#45df7e"
    else:
        return "#da5657"
#style={"color": get_color(trending_list[index][2])}

def update_row():
    rows = []
    for index in trending_list:
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
                        style={'display': 'inline-block','width': '50%'}),
                        html.P(trending_list[index][1],
                        id=index + "volume", 
                        className="trend-col",
                        style={'display': 'inline-block','width': '50%',"color": get_color(trending_list[index][2])})
                    ]
                )
            ]
        )
        rows.append(row)
    return rows

def update_info():
    infoes = []
    for index in trending_list:
        info = html.Div(
            [html.H6(trending_list[index][0],id=index+"percent"), html.P("percent: "+str(trending_list[index][2]))],
            id=index + "info",
            className="mini_container",
            style={'flex': 1},
        )
        infoes.append(info)
    return infoes

def update_Stream():
    updateData()
    return [update_row(),update_info()]

app.layout = html.Div(
    [
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
                                html.P(className="trend-col",children="Symbol",style={'display': 'inline-block','width': '50%'}),
                                html.P(className="trend-col",children="Volume",style={'display': 'inline-block','width': '50%'}),
                                html.Div(
                                    id="trending-data",
                                    className="symbol-volume",
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
                            min=1970,
                            max=2019,
                            value=[2000, 2010],
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

@app.callback(
    Output("count_graph", "figure"),
    [Input("industry", "value")],
    [
        State('date-picker-range', 'start_date'),
        State('date-picker-range', 'end_date')
    ]
)
def make_count_figure(industry,start,end):
    data = [
        dict(
            type="bar",
            x=[i for i in range(1900,1950)],
            y=[i for i in range(50)],
            name="Volatility",
            marker=dict(color=["rgb(123, 199, 255)" for i in range(50)]),
        ),
    ]
    figure = dict(data=data, layout=layout)
    return figure


@app.callback(
    Output("top_stocks", "children"),
    [Input("year_slider", "value")],
    [
        State("industry", "value"),
        State('date-picker-range', 'start_date'),
        State('date-picker-range', 'end_date')
    ]
)
def calculate_top(year_slider,industry,start,end):
    return "Top Stock:"

@app.callback(
    [
        Output("trending-data", "children"),
        Output("info-container", "children"),
    ],
    [Input("interval", "n_intervals")])
def update_trending(n):
    return update_Stream()

if __name__ == "__main__":
    update_Stream()
    app.run_server(debug=True)