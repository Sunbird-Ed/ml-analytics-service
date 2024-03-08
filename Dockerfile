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
RUN mkdir -p /opt/sparkjobs/ml-analytics-service/logs/observation/evidence
RUN mkdir -p /opt/sparkjobs/ml-analytics-service/logs/observation/status
RUN mkdir -p /opt/sparkjobs/ml-analytics-service/logs/project
RUN mkdir -p /opt/sparkjobs/ml-analytics-service/logs/project/evidence
RUN mkdir -p /opt/sparkjobs/ml-analytics-service/logs/survey
RUN mkdir -p /opt/sparkjobs/ml-analytics-service/logs/survey/evidence && chown -R analytics:analytics /opt/sparkjobs/ml-analytics-service
RUN chmod +rwx -R /opt/sparkjobs/ml-analytics-service/
COPY . /opt/sparkjobs/ml-analytics-service/
COPY faust.sh /opt/sparkjobs/faust_as_service/faust.sh
RUN chown -R analytics:analytics /opt/sparkjobs/ml-analytics-service
USER analytics
WORKDIR /opt/sparkjobs/ml-analytics-service
RUN virtualenv spark_venv
RUN /opt/sparkjobs/ml-analytics-service/spark_venv/bin/pip install --upgrade -r /opt/sparkjobs/ml-analytics-service/requirements.txt
#COPY start-services.sh .
#CMD ./start-services.sh
COPY start-faust-services.sh .
CMD ./start-faust-services.sh
