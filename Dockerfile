# syntax=docker/dockerfile:1.7-labs
FROM apache/spark:3.5.1-scala2.12-java17-python3-r-ubuntu

# theoretically these dirs should be absolute
COPY --link --chown=spark:spark . codametrix/
WORKDIR codametrix

USER root
RUN pip install virtualenv
USER spark
RUN python3.10 -m virtualenv venv
ENV VIRTUAL_ENV /env
ENV PATH venv/bin:$PATH
RUN venv/bin/pip install -r ./requirements.txt
RUN venv/bin/pip install -e .

ENTRYPOINT pytest