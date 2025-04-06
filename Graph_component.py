import dash
from dash import html
import dash_cytoscape as cyto
stylesheet=[
    {
        'selector': 'node',
        'style': {
            'shape': 'roundrectangle',
            'background-color': '#0074D9',   # dark blue color
            'label': 'data(label)',
            'color': 'white',
            'font-size': '16px',
            'text-valign': 'center',
            'text-halign': 'center',
            'width': '120px',
            'height': '40px'
        }
    },
    {
        'selector': 'edge',
        'style': {
            'width': 2,
            'line-color': '#ccc',
            'target-arrow-color': '#ccc',
            'target-arrow-shape': 'triangle'
        }
    }
]

def main():
    app = dash.Dash(__name__)
    app.title = "Graph Component Example"
    app.layout = html.Div([
        cyto.Cytoscape(
            id='flowchart',
            layout={'name': 'breadthfirst'},  # makes it a top-down tree
            style={'width': '100%', 'height': '600px'},
            elements=[
                # Nodes
                {'data': {'id': 'sort', 'label': 'Sort'}},
                {'data': {'id': 'agg1', 'label': 'Aggregate'}},
                {'data': {'id': 'agg2', 'label': 'Aggregate'}},
                {'data': {'id': 'merge', 'label': 'Merge Join'}},
                {'data': {'id': 'index1', 'label': 'Index Only Scan'}},
                {'data': {'id': 'index2', 'label': 'Index Scan'}},

                # Edges
                {'data': {'source': 'sort', 'target': 'agg1'}},
                {'data': {'source': 'agg1', 'target': 'agg2'}},
                {'data': {'source': 'agg2', 'target': 'merge'}},
                {'data': {'source': 'merge', 'target': 'index1'}},
                {'data': {'source': 'merge', 'target': 'index2'}},
            ],
            stylesheet=stylesheet
        )  
    ])
    
    app.run()

if __name__ == '__main__':
    main()
