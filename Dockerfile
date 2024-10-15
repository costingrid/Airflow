FROM apache/airflow:2.10.2

# Install JDK
USER root
RUN apt-get update && \
    apt install -y openjdk-17-jre-headless && \
    apt-get install -y ant && \
    apt-get clean;
RUN apt-get autoremove -yqq --purge
RUN rm -rf /var/lib/apt/lists/*
USER airflow

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
