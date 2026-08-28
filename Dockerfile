FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if required for psycopg2/asyncpg etc.
# asyncpg usually works out of the box with python 3.11-slim but libpq-dev is good to have.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Add /app to PYTHONPATH so imports from shared/ work correctly
ENV PYTHONPATH=/app

# The command will be overridden by docker-compose for each service
CMD ["python", "--version"]
