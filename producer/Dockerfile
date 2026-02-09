FROM python:3.10-slim
WORKDIR /app
COPY producer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/. ./
COPY data/Amazon.csv ./data/Amazon.csv
CMD ["python", "kafka_producer_amazon.py"]
