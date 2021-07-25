FROM python:3.9.5

RUN apt-get update

COPY requirements.txt /code/
RUN pip install -r /code/requirements.txt

COPY ecosia_pandas.py /code/

WORKDIR /code

CMD ["ecosia_pandas.py"]
ENTRYPOINT ["python3"]
