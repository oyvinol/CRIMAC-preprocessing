FROM python:3.10-slim-bullseye

RUN apt-get update -y 
RUN apt-get install -y git

WORKDIR /app
COPY *.py /app/
COPY requirements.txt /app
RUN pip install -r requirements.txt

CMD ["python", "CRIMAC_preprocess.py"]
