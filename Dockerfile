# Use the official Playwright Python image which comes with all dependencies
FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VCOM_HEADLESS=false \
    DISPLAY=:99

# Install system dependencies and Xvfb for non-headless mode
RUN apt-get update && apt-get install -y \
    curl \
    xvfb \
    libgbm-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (already in the image, but ensures everything is up to date)
RUN playwright install chromium

# Copy the rest of the application
COPY . .

# Expose the dashboard port
EXPOSE 8080

# Command to run the orchestrator with xvfb-run
CMD ["xvfb-run", "--server-args=-screen 0 1450x900x24", "python", "run_monitor.py"]
