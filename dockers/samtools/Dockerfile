FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    python3-dev \
    libssl-dev \
    libffi-dev \
    gcc \
    samtools \
    python3-pip \
    python3-setuptools

RUN pip3 install pika
RUN pip3 install pysam
RUN pip3 install wheel
RUN pip3 install gsutil

COPY samtools/opt/index.py /opt/.
COPY utility/utility.py /opt/.
COPY utility/conn.py /opt/.
COPY .boto /opt/.

#CMD ["gsutil"]
#CMD ["/tmp/gatk.py"]
#ENTRYPOINT ["python"]
