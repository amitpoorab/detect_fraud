FROM jupyter/all-spark-notebook:latest

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ARG user

COPY . .
ENV PYTHONPATH=$PYTHONPATH:/home/$user/app/src/main/python
VOLUME /app

 
