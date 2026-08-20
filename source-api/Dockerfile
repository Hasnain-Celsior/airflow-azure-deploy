FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY source_api.py .
COPY data/ ./data/

EXPOSE 8000
CMD ["python", "source_api.py"]
