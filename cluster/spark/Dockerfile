FROM adoptopenjdk/openjdk11:latest
EXPOSE 8081 8080 7077 6066

ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYSPARK_PYTHON=/usr/bin/python3

RUN apt update && \
    apt -y install wget tar python3 python3-pip && \
    pip3 --no-cache-dir install pyspark==3.2.1 && \
    wget https://downloads.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2-scala2.13.tgz && \
    tar xvf spark-3.2.1-bin-hadoop3.2-scala2.13.tgz && \
    mv spark-3.2.1-bin-hadoop3.2-scala2.13 /opt/ && \
    rm -rf spark-3.2.1-bin-hadoop3.2-scala2.13.tgz && \
    ln -s /opt/spark-3.2.1-bin-hadoop3.2-scala2.13 ${SPARK_HOME} && \
    cp ${SPARK_HOME}/conf/spark-defaults.conf.template ${SPARK_HOME}/conf/spark-defaults.conf && \
    echo 'spark.shuffle.service.enabled=true' >> ${SPARK_HOME}/conf/spark-defaults.conf && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.10.1/hadoop-aws-2.10.1.jar -P ${SPARK_HOME}/jars && \
    wget https://repo1.maven.org/maven2/org/jets3t/jets3t/0.9.7/jets3t-0.9.7.jar -P ${SPARK_HOME}/jars && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -P ${SPARK_HOME}/jars && \
    rm -rf /var/lib/apt/lists/*

ADD cluster/spark/conf/jets3t.properties ${SPARK_HOME}/conf
ADD cluster/spark/scripts/ ${SPARK_HOME}/bin/
