FROM quay.io/astronomer/astro-runtime:13.1.0

# Switch to root to install system packages
USER root

# Install git explicitly for dbt deps
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to astro user
USER astro

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .
