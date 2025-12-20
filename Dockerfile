FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install .

COPY operator.py .

CMD ["kopf", "run", "operator.py"]
