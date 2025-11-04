# obt-builder/Dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY build_obt.py /app/

RUN pip install --no-cache-dir psycopg2-binary

CMD ["python", "/app/build_obt.py"]
