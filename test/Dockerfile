
FROM circleci/python:latest

WORKDIR /home/circleci
COPY . /home/circleci/
RUN pip install --user -r requirements.txt

cmd timeout -k 10 600 /home/circleci/.local/bin/pytest
