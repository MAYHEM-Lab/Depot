FROM racelab/depot-spark:latest
EXPOSE 9994

ADD client /opt/depot-client
RUN pip3 --no-cache-dir install -r /opt/depot-client/requirements.txt && \
    pip3 --no-cache-dir install /opt/depot-client/ && \
    pip3 --no-cache-dir install jupyterlab==3.3.2 && \
    pip3 --no-cache-dir install boto3==1.24.18

ADD cluster/transformer/src /opt/depot-transformer
ADD kernel/src /opt/depot-kernel

ENTRYPOINT ["/opt/depot-transformer/transformer.py"]
