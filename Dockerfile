FROM python:3.10.18-slim

WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy poetry lock and pyproject files
COPY poetry.lock pyproject.toml ./

# Configure poetry to not create a virtualenv
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-root --no-dev

# Copy the rest of the application
COPY . .

# Command to run the application
CMD ["poetry", "run", "python", "src/ex7_consumer/app.py"]