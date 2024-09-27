FROM apache/airflow:slim-2.10.0-python3.11


ENV PROJECT_DIR=/Project

# Set the working directory to the project root
WORKDIR $PROJECT_DIR

# Set Poetry version and disable virtual envs create
ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VERSION=1.8.3 \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    POETRY_HOME='/usr/local' 

USER root

RUN curl -sSL https://install.python-poetry.org | python3 -

# Install Poetry and  any system-level dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 

# Copy Poetry files; Also for caching purposes
COPY pyproject.toml poetry.lock $PROJECT_DIR/

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --without dev

# Copy Project
COPY . $PROJECT_DIR
