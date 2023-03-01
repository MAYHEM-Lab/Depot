FROM racelab/depot-spark:latest
EXPOSE 8888
ADD client /opt/depot-client
RUN pip3 --no-cache-dir install -r /opt/depot-client/requirements.txt && \
    pip3 --no-cache-dir install /opt/depot-client/ && \
    pip3 --no-cache-dir install jupyterlab==3.3.2 && \
    pip3 --no-cache-dir install boto3==1.24.18


ADD cluster/executor/src /opt/depot-notebook-executor
ADD kernel/src /opt/depot-kernel

ENTRYPOINT ["/opt/depot-notebook-executor/executor.py"]
