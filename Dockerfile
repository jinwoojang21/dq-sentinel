FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[kafka]"
COPY contracts ./contracts
ENTRYPOINT ["dq-sentinel"]
CMD ["--help"]
