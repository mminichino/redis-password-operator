FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install .

COPY redis_password_operator.py .

CMD ["kopf", "run", "redis_password_operator.py"]
