FROM python:3.12-slim

WORKDIR /app

# FFmpeg — audio playback (Controller plays the extracted stream URL directly)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    unzip \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Deno — JS runtime for yt-dlp-ejs (Option C fallback extraction)
RUN curl -fsSL https://deno.land/install.sh | sh -s -- -y \
    && ln -s /root/.deno/bin/deno /usr/local/bin/deno
ENV PATH="/root/.deno/bin:${PATH}"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render injects $PORT at runtime — Flask app must bind to it, not a fixed port
EXPOSE 10000

CMD ["python", "controller.py"]
