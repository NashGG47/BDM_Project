FROM python:3.10-slim


WORKDIR /app

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY app/ .


ENV PYTHONUNBUFFERED=1


CMD ["python"]
