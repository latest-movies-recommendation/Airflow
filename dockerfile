FROM apache/airflow:2.6.3

USER root
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     vim \
#     wget \
#     unzip \
#     && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
#     && apt -y install ./google-chrome-stable_current_amd64.deb \
#     && wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/` curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip \
#     && mkdir /usr/src/chrome \
#     && unzip /tmp/chromedriver.zip chromedriver -d /usr/src/chrome \
#     && apt-get autoremove -yqq --purge \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /usr/src
COPY requirements.txt /opt/airflow/requirements.txt

USER airflow
RUN pip install -r /opt/airflow/requirements.txt
COPY dags/ /opt/airflow/dags/