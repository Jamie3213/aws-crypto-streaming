FROM python:3.10-buster

WORKDIR /app

COPY src/ .
COPY requirements.txt .

RUN pip install \
--trusted-host pypi.org \
--trusted-host pypi.python.org \
--trusted-host=files.pythonhosted.org \
-r requirements.txt

CMD ["python3", "app.py"]
