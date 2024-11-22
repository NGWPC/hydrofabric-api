import os
import sys
from pathlib import Path

# Get the root directory of your project
root_dir = Path(__file__).parent.parent

# Add the root directory to Python path
sys.path.append(str(root_dir))
