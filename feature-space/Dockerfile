FROM python:3-stretch

# Install OpenJDK 8, and monitoring tooling
RUN \
    apt-get update && \
    apt-get install -y openjdk-8-jdk htop bmon && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade setuptools
COPY requirements.txt /
RUN pip install -r /requirements.txt

# Add dependency for hadoop-azure
ADD https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/2.0.0/azure-storage-2.0.0.jar /usr/local/lib/python3.7/site-packages/pyspark/jars/
# Add hadoop-azure to access Azure Blob Storage
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/2.7.3/hadoop-azure-2.7.3.jar /usr/local/lib/python3.7/site-packages/pyspark/jars/

ENV PATH=$PATH:/src
ENV PYTHONPATH /src

ADD ./ /src
WORKDIR /src/

EXPOSE 5000
ENTRYPOINT ["python3"]
CMD ["feature-space.py"]