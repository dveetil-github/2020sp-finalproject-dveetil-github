FROM puckel/docker-airflow:1.10.9 AS base
ARG CI_USER_TOKEN
RUN echo "machine github.com\n  login $CI_USER_TOKEN\n" >~/.netrc
ENV \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    PIP_SRC=/src \
    PIPENV_HIDE_EMOJIS=true \
    PIPENV_COLORBLIND=true \
    PIPENV_NOSPIN=true

USER root
RUN apt-get update && \
apt-get install -y git build-essential curl wget software-properties-common zip unzip
RUN pip install dask[dataframe]==2.15.0
RUN pip install boto3
RUN pip install matplotlib

USER airflow
mkdir /usr/local/airflow/data
mkdir /usr/local/airflow/completed
# Just for documentation. Expose webserver
EXPOSE 8080

