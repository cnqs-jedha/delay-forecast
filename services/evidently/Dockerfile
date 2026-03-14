FROM python:3.11-slim
LABEL maintainer="delay-forecast-team"

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV PYTHONPATH=/app 

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev curl && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
RUN pip install evidently==0.4.0 --force-reinstall
    
COPY requirements.txt .

RUN pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir evidently uvicorn

COPY services/evidently/app ./app
RUN mkdir -p /app/reports

EXPOSE 8001
# On lance uvicorn depuis /app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]