FROM racelab/depot-spark:latest
EXPOSE 9995

ADD client /opt/depot-client
RUN pip3 --no-cache-dir install -r /opt/depot-client/requirements.txt && \
    pip3 --no-cache-dir install /opt/depot-client/ && \
    pip3 --no-cache-dir install tornado==6.1

ADD cluster/router/src /opt/depot-notebook-router

ENTRYPOINT ["/opt/depot-notebook-router/router.py"]
