FROM python:3.11-alpine

WORKDIR /app

COPY . .

ENV USER = neo4j
ENV PASS = password
ENV URI = bolt://localhost:7687
ENV INSIDE_DOCKER = true

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["python3", "main.py"]