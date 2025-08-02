#!/usr/bin/env python3
"""
Script to run the Elasticsearch Query Generator Streamlit app.
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Run the Streamlit app."""
    script_path = Path(__file__).parent / "streamlit_viewer.py"
    
    try:
        # Run streamlit with the viewer script
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(script_path),
            "--server.port=8551",
            "--server.address=0.0.0.0"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Streamlit: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nStreamlit app stopped by user")
        sys.exit(0)

if __name__ == "__main__":
    main() 