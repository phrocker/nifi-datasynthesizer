FROM phrocker/nifi11:latest
ENV PYTHONUNBUFFERED 1
WORKDIR /opt/nifi/nifi-current/
COPY nifi-accumulo-services-api-nar-1.10.0.nar /opt/nifi/nifi-current/extensions/
COPY nifi-accumulo-services-nar-1.10.0.nar  /opt/nifi/nifi-current/extensions/
COPY nifi-accumulo-nar-1.10.0.nar  /opt/nifi/nifi-current/extensions/
