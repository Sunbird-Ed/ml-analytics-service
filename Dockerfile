#Official Ubuntu Image
FROM ubuntu:20.04
#Install necessary packages
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y \
    openjdk-8-jdk \
    software-properties-common \
    python3-pip \
    python3-venv \
    python3-virtualenv \
    zip \
    unzip \
    acl
#Create the User
RUN useradd -m -s /bin/bash analytics
RUN mkdir -p /opt/sparkjobs/ml-analytics-service && chown analytics:analytics /opt/sparkjobs/ml-analytics-service
USER analytics
WORKDIR /opt/sparkjobs/ml-analytics-service
RUN virtualenv spark_venv
COPY . /opt/sparkjobs/ml-analytics-service/
RUN /opt/sparkjobs/ml-analytics-service/spark_venv/bin/pip install --upgrade -r /opt/sparkjobs/ml-analytics-service/requirements.txt
COPY faust.sh /opt/sparkjobs/faust_as_service/faust.sh
WORKDIR /opt/sparkjobs
COPY start-services.sh .
CMD ./start-services.sh
