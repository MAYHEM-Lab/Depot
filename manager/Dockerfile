FROM adoptopenjdk/openjdk14:latest
EXPOSE 8888
RUN apt update && \
    apt -y install python git && \
    git clone https://github.com/eucalyptus/euca2ools.git && \
    cd euca2ools && \
    curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py && \
    python2 get-pip.py && \
    pip2 install pathlib certifi==2021.10.8 && \
    python2 setup.py install

ADD manager/target/scala-2.13/server.jar /opt/depot-manager/

ENTRYPOINT ["java", "-jar", "/opt/depot-manager/server.jar"]
