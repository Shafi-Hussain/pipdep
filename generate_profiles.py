"""
To be run in the target environment (native platform)
Depends On: packaging
    pip install packaging
Usage:
    python generate_profiles.py myprofile
"""
import sys
from packaging.tags import sys_tags

with open(sys.argv[1], "w") as f:
    f.write("\n".join(map(str, sys_tags())))
