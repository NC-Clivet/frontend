FROM python:3.13

WORKDIR /app

# Installazione dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia tutto il codice (incluso app.py)
COPY . .

# Espone la porta interna
EXPOSE 8501

# Comando di avvio con il nome file corretto: app.py
CMD ["streamlit", "run", "app.py", \
    "--server.port=8501", \
    "--server.address=0.0.0.0", \
    "--server.headless=true", \
    "--server.enableCORS=false", \
    "--server.enableXsrfProtection=false", \
    "--browser.serverAddress=docker.clivet.it", \
    "--browser.serverPort=7443"]
