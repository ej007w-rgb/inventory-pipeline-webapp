FROM python:3.12-slim
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Render (and other hosts) assign the port via $PORT; default 7860 for Hugging Face.
# Single worker is deliberate: background job state lives in this process.
EXPOSE 7860
CMD ["sh", "-c", "gunicorn -w 1 --threads 8 --timeout 600 -b 0.0.0.0:${PORT:-7860} app:app"]
