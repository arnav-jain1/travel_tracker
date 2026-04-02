FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

COPY db.py .
RUN mkdir -p /app/data

EXPOSE 5000

# Note: Since the file is named db.py, the module is 'db' instead of 'backend'
CMD ["sh", "-c", "python -c 'import db; db.init_db()' && gunicorn -w 4 -b 0.0.0.0:5000 db:app"]
