FROM python:3.12

WORKDIR /app

# Install Requisites
RUN apt-get update && apt-get install -y \
    poetry

# Setup project
COPY news_extractor_engine pyproject.toml /app/
RUN poetry config virtualenvs.create false
RUN poetry install
RUN mkdir logs

# Run project
CMD [ "python", "./news_extractor_engine/main.py" ]