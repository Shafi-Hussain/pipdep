"""
To be run in the target environment (native platform)
Depends On: packaging
    pip install packaging
Usage:
    python generate_environment.py env.json
"""
import json, sys
from packaging.markers import default_environment

with open(sys.argv[1], "w") as f:
    json.dump(default_environment(), f)
