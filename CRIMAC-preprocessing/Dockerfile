FROM python:3.8 as builder

RUN mkdir /install
WORKDIR /install

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt

COPY pyEcholab/ /pyecholab/
RUN pip install -e /pyecholab

COPY *.py /app/

WORKDIR /app

CMD ["python3", "/app/CRIMAC_preprocess.py"]
