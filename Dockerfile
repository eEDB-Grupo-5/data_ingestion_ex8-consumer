FROM python:3.12-slim

WORKDIR /app

ENV DEBIAN_FRONTEND=noninteractive

ARG YOUR_ENV=production

ENV YOUR_ENV=${YOUR_ENV} \
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  # Poetry's configuration:
  POETRY_NO_INTERACTION=1 \
  POETRY_VIRTUALENVS_CREATE=false \
  POETRY_CACHE_DIR='/var/cache/pypoetry' \
  POETRY_HOME='/usr/local' \
  POETRY_VERSION=2.2.0

# Install Java, wget and tar (use the distro default JDK to avoid package name mismatches)
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
	   default-jdk-headless \
	   wget \
	   tar \
	   curl \
	   git \
	   ca-certificates \
	   build-essential \
	   python3-dev \
	   libssl-dev \
	   libffi-dev \
	   pkg-config \
	   maven \
	&& rm -rf /var/lib/apt/lists/*

# System deps:
RUN curl -sSL https://install.python-poetry.org | python3 -

# Copy project files
COPY . /app

RUN poetry lock
RUN poetry install $(test "$YOUR_ENV" == production && echo "--only=main") --no-interaction --no-ansi

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy project files
COPY . /app

# Expose logs (optional)
VOLUME ["/app/logs"]

ENV JAVA_HOME=${JAVA_HOME}
