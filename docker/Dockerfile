
FROM ubuntu:latest

MAINTAINER Iain Mackie

USER root

# Install essentials
RUN apt-get update && apt-get install -y \
    software-properties-common  \
    wget \
    make \
    gcc  \
    git

# Install java 8
RUN apt-get install openjdk-8-jdk -y
ENV JAVA_VERSION="java-8-openjdk-amd64"
ENV JAVA_HOME=/usr/lib/jvm/$JAVA_VERSION
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install scala
RUN apt-get install scala -y

# Install python
RUN apt-get install -y python3 \
    python3-pip \
    python-setuptools
RUN pip3 install \
    jupyter \
    jupyterlab \
    py4j \
    cbor \
    transformers \
    spacy \
    torch

# Install Spark
ENV APACHE_SPARK_VERSION=2.4.5 \
    HADOOP_VERSION=2.7
RUN cd /tmp && \
    wget -q $(wget -qO- https://www.apache.org/dyn/closer.lua/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz\?as_json | \
    python3 -c "import sys, json; content=json.load(sys.stdin); print(content['preferred']+content['path_info'])") && \
    echo "2426a20c548bdfc07df288cd1d18d1da6b3189d0b78dee76fa034c52a4e02895f0ad460720c526f163ba63a17efae4764c46a1cd8f9b04c60f9937a554db85d2 *spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | sha512sum -c - && \
    tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local/share/ --owner root --group root --no-same-owner && \
    rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
ENV SPARK_HOME=/usr/local/share/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info"
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3

# Install github code for TREC CAR entity processing
RUN git config --global user.email "i.mackie@me.com"
RUN git config --global user.name "iain-mackie"
