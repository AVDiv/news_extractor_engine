FROM python:3.12

WORKDIR /app

# Install Requisites
RUN curl -sSL https://install.python-poetry.org | python3 -

# Setup project
COPY pyproject.toml .
COPY news_extractor_engine ./news_extractor_engine
RUN ~/.local/bin/poetry config virtualenvs.create false
RUN ~/.local/bin/poetry install
RUN mkdir logs


# Run project
CMD [ "python", "news_extractor_engine/main.py" ]