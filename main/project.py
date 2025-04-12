"""
Main entry point for launching the SQL Query Transformer Dash App.
This script starts the interface defined in interface.py.
"""

import interface

if __name__ == "__main__":
    interface.app.run(debug=True)