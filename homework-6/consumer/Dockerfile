FROM python:3.12-slim

WORKDIR /app

COPY consumer/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/consumer.py .
COPY schemas /app/schemas

CMD ["python", "consumer.py"]