import os
from pathlib import Path
import secrets


path = Path(__file__).resolve().parents[1] / ".env"
with path.open("x") as file:
    os.chmod(path, 0o600)
    file.write(f"POSTGRES_PASSWORD={secrets.token_hex(24)}\n")
    file.write(f"JWT_SECRET={secrets.token_hex(32)}\n")
