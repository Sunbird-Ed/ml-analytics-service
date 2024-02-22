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
RUN mkdir -p /opt/sparkjobs/ml-analytics-service && chown -R analytics:analytics /opt/sparkjobs/ml-analytics-service
USER analytics
WORKDIR /opt/sparkjobs/ml-analytics-service
COPY . /opt/sparkjobs/ml-analytics-service/
COPY faust.sh /opt/sparkjobs/faust_as_service/faust.sh
RUN virtualenv spark_venv
RUN /opt/sparkjobs/ml-analytics-service/spark_venv/bin/pip install --upgrade -r /opt/sparkjobs/ml-analytics-service/requirements.txt
COPY start-services.sh .
CMD ./start-services.sh
