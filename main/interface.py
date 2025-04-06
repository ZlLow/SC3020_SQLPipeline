import dash
from dash import html, Input, Output, State
import dash_bootstrap_components as dbc
import dash_ace

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
server = app.server

app.layout = dbc.Container([
    html.H1("SQL Query Transformer", className="mt-4 text-center"),

    html.P(
        "Input a SQL query below to view its execution plan and a pipe-syntax version of the query.",
        className="mb-3 text-center text-secondary"
    ),

    html.Hr(style={'borderTop': '2px solid #bbb'}),

    dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.H5("SQL Query Input"),
                    dash_ace.DashAceEditor(
                        id='sql-input',
                        value='SELECT * FROM users;',
                        theme='monokai',
                        mode='sql',
                        style={'height': '600px', 'width': '100%'},
                        tabSize=4,
                        showPrintMargin=True,
                        fontSize=14,
                    ),
                    dbc.Button("Submit", id='submit-btn', color="primary", className="mt-3")
                ], width=4, className="p-2"),

                dbc.Col([
                    html.Div("➜", style={
                        'fontSize': '48px',
                        'textAlign': 'center'
                    })
                ], width=1, style={
                    'display': 'flex',
                    'alignItems': 'center',
                    'justifyContent': 'center',
                    'height': '600px'
                }),

                dbc.Col([
                    html.H5("Query Execution Plan"),
                    dbc.Textarea(
                        id='query-plan-output',
                        style={'height': '600px', 'fontFamily': 'monospace', 'fontSize': '14px'},
                        readOnly=True
                    )
                ], width=3, className="p-2"),

                dbc.Col([
                    html.H5("Pipe Syntax Version"),
                    dbc.Textarea(
                        id='pipe-syntax-output',
                        style={'height': '600px', 'fontFamily': 'monospace', 'fontSize': '14px'},
                        readOnly=True
                    )
                ], width=4, className="p-2")
            ])
        ])
    ], className="shadow-sm p-3 my-4 bg-white rounded")
], fluid=True, style={'backgroundColor': '#f8f9fa', 'minHeight': '100vh'})


@app.callback(
    Output('query-plan-output', 'value'),
    Output('pipe-syntax-output', 'value'),
    Input('submit-btn', 'n_clicks'),
    State('sql-input', 'value'),
    prevent_initial_call=True
)
def transform_sql(n_clicks, sql_input):
    if not sql_input:
        return "No SQL input provided.", ""

    query_plan = f"-- Execution plan for:\n{sql_input}"
    pipe_syntax = f"-- Pipe syntax for:\n{sql_input.replace('SELECT', 'df |> select', 1)}"

    return query_plan, pipe_syntax


if __name__ == '__main__':
    app.run(debug=True)
