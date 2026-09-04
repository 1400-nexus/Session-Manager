import sys
from pathlib import Path

GENERATED_PROTO_PATH = Path(__file__).resolve().parent.parent / "libs/nexus-proto/generated/python"

if str(GENERATED_PROTO_PATH) not in sys.path:
    sys.path.insert(0, str(GENERATED_PROTO_PATH))
