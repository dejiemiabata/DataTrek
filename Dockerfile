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

# Install any system-level dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -


# Copy Poetry files; Also for caching purposes
COPY pyproject.toml poetry.lock $PROJECT_DIR/

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --without dev

# Copy Project
COPY . $PROJECT_DIR
