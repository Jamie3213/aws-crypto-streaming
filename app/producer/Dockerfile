FROM python:3.10-buster

WORKDIR /app

COPY src/ .
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python3", "app.py"]
