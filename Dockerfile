FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .

RUN mkdir redis_password_operator
COPY redis_password_operator/__init__.py redis_password_operator/
COPY redis_password_operator/redis_password_operator.py redis_password_operator/

RUN pip install .

CMD ["kopf", "run", "-m", "redis_password_operator.redis_password_operator"]
