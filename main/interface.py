import dash
from dash import html, Input, Output, State
import dash_bootstrap_components as dbc
import dash_ace
import dash_cytoscape as cyto
from preprocessing import DBConnection, QEP
from pipesyntax import Parser, QueryType

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
                        style={
                            'minHeight': '500px',
                            'width': '100%',
                            'height': '100%'
                        },
                        tabSize=4,
                        showPrintMargin=True,
                        fontSize=14,
                    ),
                    html.Div(id='error-message', className='mt-2', style={'minHeight': '24px'}),
                    dbc.Button("Submit", id='submit-btn', color="primary", className="mt-3")
                ], width=4, className="p-2"),

                dbc.Col([
                    html.Div("➜", style={
                        'fontSize': '48px',
                        'textAlign': 'center'
                    })
                ], width=1, style={
                    'height': '550px',
                    'display': 'flex',
                    'alignItems': 'center',
                    'justifyContent': 'center',
                }),

                dbc.Col([
                    html.H5("QEP Diagram"),
                    cyto.Cytoscape(
                        id='qep-graph',
                        layout={
                            'name': 'breadthfirst',
                            'directed': True,
                            'spacingFactor': 0.8,
                            'padding': 0,
                            'roots': '[id = "0_LIMIT"]',
                        },
                        style={
                            'minHeight': '500px',
                            'width': '100%',
                            'border': '1px solid #ccc',
                            'borderRadius': '10px',
                            'backgroundColor': '#fdfdfd'
                        },
                        elements=[],
                        stylesheet=[
                            {
                                'selector': 'node',
                                'style': {
                                    'label': 'data(label)',
                                    'text-wrap': 'wrap',
                                    'text-max-width': '150px',
                                    'shape': 'roundrectangle',
                                    'background-color': '#0074D9',
                                    'color': 'white',
                                    'font-size': '12px',
                                    'padding': '8px',
                                    'width': 'label',
                                    'height': 'label',
                                    'text-valign': 'center',
                                    'text-halign': 'center'
                                }
                            },
                            {
                                'selector': 'edge',
                                'style': {
                                    'curve-style': 'bezier',
                                    'target-arrow-shape': 'triangle',
                                    'arrow-scale': 1,
                                    'line-color': '#ccc',
                                    'target-arrow-color': '#ccc',
                                    'width': 2
                                }
                            }
                        ],
                        zoomingEnabled=False,
                        userZoomingEnabled=False,
                    )
                ], width=5, className="p-2"),

                dbc.Col([
                    html.H5("Pipe Syntax Version"),
                    dbc.Textarea(
                        id='pipe-syntax-output',
                        style={
                            'minHeight': '500px',
                            'width': '100%',
                            'height': '100%',
                            'fontFamily': 'monospace',
                            'fontSize': '14px'
                        },
                        readOnly=True
                    )
                ], width=2, className="p-2")
            ], style={'alignItems': 'flex-start'})
        ])
    ], className="shadow-sm p-3 my-4 bg-white rounded")
], fluid=True, style={'backgroundColor': '#f8f9fa', 'minHeight': '100vh'})


def qep_to_graph_elements(unfiltered_qep_list):
    elements = []
    node_id_map = {}
    node_counter = 0
    join_children_map = {}

    qep_list = [step for step in unfiltered_qep_list if list(step.keys())[0].name != 'WHERE']

    for step in qep_list:
        query_type, metadata = list(step.items())[0]
        node_id = f"{node_counter}_{query_type.name}"

        def fmt(k, v):
            s = str(v)
            if len(s) > 40:
                # Add newlines after logical operators for better wrapping
                s = s.replace(" AND ", "\nAND ")
                s = s.replace(" OR ", "\nOR ")
                s = s.replace(" THEN ", "\nTHEN ")
                s = s.replace(" ELSE ", "\nELSE ")
            return f"{k}: {s}"

        label_lines = [fmt(k, v) for k, v in metadata.items()]
        label = f"{query_type.name}\n" + "\n".join(label_lines)

        elements.append({
            'data': {'id': node_id, 'label': label},
            'position': {'x': 0, 'y': node_counter * 220}
        })

        node_id_map[node_counter] = node_id
        node_counter += 1

    for idx, step in enumerate(qep_list):
        query_type = list(step.keys())[0]
        if query_type.name == 'JOIN':
            from_indices = [i for i in range(idx + 1, len(qep_list))
                            if list(qep_list[i].keys())[0].name == 'FROM'][:2]
            join_children_map[node_id_map[idx]] = [node_id_map[i] for i in from_indices]

    for join_id, children in join_children_map.items():
        if len(children) == 2:
            elements = [
                {
                    **el,
                    'position': {
                        **el['position'],
                        'x': -300 if el['data']['id'] == children[0] else
                             300 if el['data']['id'] == children[1] else el['position']['x']
                    }
                } if el['data']['id'] in children else el
                for el in elements
            ]

    for idx, step in enumerate(qep_list):
        query_type = list(step.keys())[0]
        this_id = node_id_map[idx]

        if query_type.name == 'JOIN':
            from_indices = [i for i in range(idx + 1, len(qep_list))
                            if list(qep_list[i].keys())[0].name == 'FROM'][:2]
            for from_idx in from_indices:
                if from_idx in node_id_map:
                    elements.append({
                        'data': {
                            'source': this_id,
                            'target': node_id_map[from_idx]
                        }
                    })
        elif idx < len(qep_list) - 1:
            next_id = node_id_map[idx + 1]
            if any(e['data'].get('target') == next_id for e in elements if 'data' in e):
                continue
            elements.append({
                'data': {
                    'source': this_id,
                    'target': next_id
                }
            })

    return elements


@app.callback(
    Output('pipe-syntax-output', 'value'),
    Output('qep-graph', 'elements'),
    Output('qep-graph', 'style'),
    Output('qep-graph', 'layout'),
    Output('error-message', 'children'),
    Input('submit-btn', 'n_clicks'),
    State('sql-input', 'value'),
    prevent_initial_call=True
)
def transform_sql(n_clicks, sql_input):
    if not sql_input:
        return "No SQL input provided.", [], {}

    try:
        db = DBConnection()
        qep_list, execution_time = QEP.unwrap(sql_input, db)
        db.close()

        pipe_syntax = Parser.parse_query(qep_list)
        graph_elements = qep_to_graph_elements(qep_list)
        node_count = len([e for e in graph_elements if 'target' not in e['data']])
        height_px = max(600, 1200 + node_count * 100)

        style = {
            'width': '100%',
            'height': f'{height_px}px',
            'border': '1px solid #ccc',
            'borderRadius': '10px',
            'backgroundColor': '#fdfdfd'
        }

        # force layout to change on every query to "reset" diagram
        first_query_type = list(qep_list[0].keys())[0]
        root_node = f"0_{first_query_type.name}"

        layout = {
            'name': 'breadthfirst',
            'directed': True,
            'spacingFactor': 0.8,
            'padding': 0,
            'roots': f'[id = "{root_node}"]'
        }

        return pipe_syntax, graph_elements, style, layout, ""

    except Exception as e:
        return "", [], {}, {}, html.Div(f"❌ Error: {str(e)}", style={"color": "red", "marginTop": "10px"})

