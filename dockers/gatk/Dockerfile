# Using OpenJDK 8
FROM broadinstitute/gatk

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev

RUN pip install pika
RUN pip install gsutil

COPY gatk/opt/gatk.py /tmp/.
COPY utility/conn.py /tmp/.
COPY utility/utility.py /tmp/.
COPY .boto /tmp/.

#CMD ["gsutil"]
#CMD ["/tmp/gatk.py"]
#ENTRYPOINT ["python"]
