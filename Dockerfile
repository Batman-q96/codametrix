# syntax=docker/dockerfile:1.7-labs
FROM apache/spark:3.5.1-scala2.12-java17-python3-r-ubuntu

# theoretically these dirs should be absolute
COPY --link --chown=spark:spark . codametrix/
WORKDIR codametrix
# RUN python3.10 -m venv venv
# RUN source venv/bin/activate

USER root
RUN pip install virtualenv
USER spark
RUN python3.10 -m virtualenv venv
ENV VIRTUAL_ENV /env
ENV PATH venv/bin:$PATH
RUN venv/bin/pip install -r ./requirements.txt
RUN venv/bin/pip install -e .

ENTRYPOINT pytest

# USER root
# RUN apt-get -y install python3.10-venv

# necessary for in container development
# USER root
# RUN apt-get -y update
# RUN apt-get -y install git

# reset to default user
# USER 185