FROM python:3.9
LABEL authors="csiha"

WORKDIR /app

COPY ./main.py /app/main.py
COPY ./task1.py /app/task1.py
COPY ./__init__.py /app/__init__.py
COPY ./tests /app/tests
COPY ./requirements.txt /app/requirements.txt

ENV TSV_PATH=/app/data

RUN apt-get update
RUN apt-get install default-jdk -y
RUN pip install -r requirements.txt

#RUN apt-get install htop -y
#ENTRYPOINT htop

#ENTRYPOINT pytest && pylint Task1
ENTRYPOINT pytest && find . -type f -name "*.py" | xargs pylint