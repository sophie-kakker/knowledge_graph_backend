FROM python:3.6

# layer caching for faster builds
COPY requirements.txt /
RUN pip install -r /requirements.txt

#COPY app.py /app.py
ADD . /flask_app
WORKDIR /flask_app

RUN chmod +x ./wait-for-it.sh
RUN echo "Echoing python path: "
RUN echo $PYTHONPATH

ENV FLASK_ENV=development

CMD flask run --host=0.0.0.0 --port=8081