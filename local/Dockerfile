FROM python:3.7.7-slim-buster

ENV GOOGLE_APPLICATION_CREDENTIALS 'local/credentials/service-account/alicorp-pe-is-qlik-gsa-local.json'
ENV PIP_DEFAULT_TIMEOUT 100
ENV PIP_DISABLE_PIP_VERSION_CHECK 'on'
ENV PIP_NO_CACHE_DIR 'on'
ENV PYTHON_ENV 'local'

RUN ln -sf /usr/share/zoneinfo/America/Lima /etc/localtime

WORKDIR /app
COPY requirements.txt .
RUN \
	pip install pip==20.0.2 && \
	pip install -r requirements.txt

CMD python main.py