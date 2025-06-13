# Enhanced Fitbit to InfluxDB Pipeline
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV TZ=UTC

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    tzdata \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd -r fitbit && useradd -r -g fitbit -u 1000 fitbit

# Create necessary directories
RUN mkdir -p /app/logs /app/tokens /app/config \
    && chown -R fitbit:fitbit /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY fitbit_to_influx.py .
COPY config/ ./config/
COPY scripts/ ./scripts/

# Make scripts executable
RUN chmod +x scripts/*.sh

# Set proper ownership
RUN chown -R fitbit:fitbit /app

# Switch to non-root user
USER fitbit

# Health check
HEALTHCHECK --interval=5m --timeout=30s --start-period=5m --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Create startup script
COPY --chown=fitbit:fitbit <<EOF /app/start.sh
#!/bin/bash
set -e

echo "Starting Fitbit to InfluxDB Pipeline..."
echo "Timestamp: $(date)"
echo "User: $(whoami)"
echo "Working Directory: $(pwd)"

# Validate environment variables
if [ -z "\$FITBIT_CLIENT_ID" ]; then
    echo "ERROR: FITBIT_CLIENT_ID is not set"
    exit 1
fi

if [ -z "\$FITBIT_CLIENT_SECRET" ]; then
    echo "ERROR: FITBIT_CLIENT_SECRET is not set"
    exit 1
fi

if [ -z "\$INFLUX_URL" ]; then
    echo "ERROR: INFLUX_URL is not set"
    exit 1
fi

if [ -z "\$INFLUX_TOKEN" ]; then
    echo "ERROR: INFLUX_TOKEN is not set"
    exit 1
fi

# Test connectivity
echo "Testing InfluxDB connectivity..."
curl -f "\$INFLUX_URL/ping" || {
    echo "WARNING: InfluxDB not reachable, will retry..."
    sleep 30
}

# Run the application
echo "Starting Fitbit data collection..."
if [ "\$SYNC_INTERVAL" ]; then
    echo "Running in scheduled mode with interval: \$SYNC_INTERVAL seconds"
    while true; do
        python fitbit_to_influx.py
        echo "Sync completed. Waiting \$SYNC_INTERVAL seconds..."
        sleep "\$SYNC_INTERVAL"
    done
else
    echo "Running in single execution mode"
    python fitbit_to_influx.py
fi
EOF

RUN chmod +x /app/start.sh

# Expose port for health checks
EXPOSE 8080

# Set default command
CMD ["/app/start.sh"]

# Add labels for better container management
LABEL maintainer="Your Name <your.email@example.com>"
LABEL description="Enhanced Fitbit to InfluxDB Pipeline for Health Monitoring"
LABEL version="2.0"
LABEL org.label-schema.build-date="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
LABEL org.label-schema.name="fitbit-influxdb-pipeline"
LABEL org.label-schema.description="Comprehensive health data pipeline for Fitbit to InfluxDB"
LABEL org.label-schema.version="2.0"
LABEL org.label-schema.schema-version="1.0"