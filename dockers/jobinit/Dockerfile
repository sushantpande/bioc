FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         python3-pip \
         python3-setuptools 

#WORKDIR /tmp
RUN pip3 install pika
RUN pip3 install --upgrade google-cloud-storage

COPY jobinit/opt/brokering.py /opt/.
COPY utility/conn.py /opt/.
COPY utility/utility.py /opt/.
COPY jobinit/opt/creds.json /mnt/.

CMD ["/opt/brokering.py"]
ENTRYPOINT ["python3"]
