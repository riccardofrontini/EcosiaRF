FROM python:3.9.5

RUN apt-get update

COPY requirements.txt /code/
RUN pip install -r /code/requirements.txt

COPY main.py /code/

WORKDIR /code

CMD ["maintenance.py"]
ENTRYPOINT ["python3"]
