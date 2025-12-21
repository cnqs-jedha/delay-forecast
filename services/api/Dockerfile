FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY services/api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/api/app ./app
COPY libs ./libs

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
