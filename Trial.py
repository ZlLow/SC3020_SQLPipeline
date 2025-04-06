from dash import html, Dash, dcc, css
from dash_bootstrap_components.themes import BOOTSTRAP
from dash.dependencies import Input, Output

def create_layout(app: Dash) -> html.Div:
    return html.Div(
        className = "app-div",
        children=[
            html.H1(app.title, className="app-header")
        ]
    )

def main():
    # Create a Dash app
    app = Dash(__name__)
    app.title = "Dashboard Example"
    app.layout = create_layout(app)
    app.run()

# Run the app
if __name__ == '__main__':
    main()