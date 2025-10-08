FROM python:3.11-slim

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir poetry && poetry install --no-root

# Default: FastAPI mode
CMD ["poetry", "run", "uvicorn", "flightrobustness.interfaces.api:app", "--host", "0.0.0.0", "--port", "8000"]
