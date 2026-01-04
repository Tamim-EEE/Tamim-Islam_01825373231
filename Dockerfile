# HL7 SIU Parser Docker Image
#
# Build:   docker build -t hl7-parser .
# Run:     docker run -v $(pwd)/samples:/data hl7-parser /data/single_appointment.hl7

FROM python:3.11-slim

LABEL maintainer="Healthcare Integration Team"
LABEL description="HL7 SIU S12 Appointment Parser"

# Set working directory
WORKDIR /app

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash appuser

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY hl7_parser.py .

# Change ownership to non-root user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Set Python path
ENV PYTHONPATH=/app

# Default entrypoint
ENTRYPOINT ["python", "hl7_parser.py"]

# Show help by default
CMD ["--help"]
