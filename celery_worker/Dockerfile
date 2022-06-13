FROM bitnami/pytorch:latest

# layer caching for faster builds
COPY ./celery_worker/requirements.txt /
RUN pip install -v -r /requirements.txt

#COPY app.py /app.py
USER root

ADD ./celery_worker /celery_worker
WORKDIR /celery_worker

ENTRYPOINT celery -A celery_task_app.worker worker -l info --concurrency=1