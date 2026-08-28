FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy the shared logging library
COPY shared/ /app/shared/
COPY services/reconciliation_worker/ /app/
CMD ["python", "main.py"]
