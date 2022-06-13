import importlib
import logging
from abc import ABC
from celery import Task
from .worker import app


class IngestionTask(Task, ABC):
    """
    Abstraction of Celery's Task class to support loading ML model.
    """
    abstract = True

    def __init__(self):
        super().__init__()
        self.model = None

    def __call__(self, *args, **kwargs):
        """
        Load model on first call (i.e. first task processed)
        Avoids the need to load model on each task request
        """
        if not self.model:
            logging.info('Loading Model...')
            module_import = importlib.import_module(self.path[0])
            logging.info('module_import: {}'.format(module_import))
            logging.info('self path[0]: {} path[1]: {}'.format(self.path[0], self.path[1]))
            model_obj = getattr(module_import, self.path[1])
            self.model = model_obj()
            logging.info('Model loaded')
        return self.run(*args, **kwargs)


@app.task(ignore_result=False,
          bind=True,
          base=IngestionTask,
          path=('celery_task_app.ml_model.re_model', 'RelationExtractionModel'),
          name='{}.{}'.format(__name__, 'RelationExtraction'))
def ingest_relations(self, *args):
    """
    Essentially the run method of IngestionTask
    """
    json_data = args[0]
    output = self.model.process_data(json_data['data'])
    return output
