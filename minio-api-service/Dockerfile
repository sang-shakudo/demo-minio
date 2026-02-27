FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY run.sh .

EXPOSE 8787

CMD ["python", "main.py"]
