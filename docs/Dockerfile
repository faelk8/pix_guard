FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /workspace
COPY docs/requirements.txt /workspace/requirements.txt
RUN pip install --no-cache-dir -r /workspace/requirements.txt

WORKDIR /workspace/docs
CMD ["sphinx-build", "-E", "-a", "-W", "--keep-going", "-b", "html", "source", "_build/html"]
