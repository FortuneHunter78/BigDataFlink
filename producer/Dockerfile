FROM python:3.9-slim

WORKDIR /app

COPY producer.py /app/producer.py

RUN pip install kafka-python

CMD ["python", "-u", "producer.py"]
