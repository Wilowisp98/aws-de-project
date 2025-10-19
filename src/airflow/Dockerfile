FROM apache/airflow:2.10.2-python3.12

USER root

RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install UV package manager
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod +x /install.sh && /install.sh && rm /install.sh

# Set up UV environment path
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /opt/airflow

COPY requirements.txt ./

RUN uv pip install --system --requirement requirements.txt

COPY . .

EXPOSE 8080

CMD ["webserver"]
