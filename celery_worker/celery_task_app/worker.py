from celery import Celery

app = Celery(
    'celery_app',
    broker='amqp://admin:mypass@rabbit:5672',
    backend='mongodb://mongodb_container:27017/mydb',
    include=['celery_task_app.tasks']
)