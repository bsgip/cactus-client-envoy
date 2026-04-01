import os

# envoy.server.settings instantiates AppSettings() at module load time, which requires DATABASE_URL.
# Set a placeholder here so imports succeed during collection; integration fixtures override it with the real URL.
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://placeholder:placeholder@localhost/placeholder")
