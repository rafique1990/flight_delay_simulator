FROM python:3.11-slim

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir uv && uv pip install --system -e .

# Default: FastAPI mode
CMD ["uvicorn", "flightrobustness.interfaces.api:app", "--host", "0.0.0.0", "--port", "8000"]
