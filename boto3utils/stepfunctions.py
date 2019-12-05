import boto3
import json
import logging

from botocore.client import Config
from botocore.vendored.requests.exceptions import ReadTimeout
from traceback import format_exc

logger = logging.getLogger(__name__)


class stepfunctions(object):

    def __init__(self, session=None):
        config = Config(read_timeout=70)
        if session is None:
            self.sfn = boto3.client('stepfunctions', config=config)
        else:
            self.sfn = session.client('stepfunctions', config=config)

    def run_activity(self, process, arn, **kwargs):
        """ Run an activity around the process function provided """
        while True:
            logger.info('Querying for task')
            try:
                task = self.sfn.get_activity_task(activityArn=arn)
            except ReadTimeout:
                logger.warning('Activity read timed out')
                continue
            token = task.get('taskToken', None)
            if token is None:
                continue
            logger.debug('taskToken: %s' % token)
            try:
                payload = task.get('input', '{}')
                logger.info('Payload: %s' % payload)
                # run process function with payload as kwargs
                output = process(json.loads(payload))
                # Send task success
                self.sfn.send_task_success(taskToken=token, output=json.dumps(output))
            except Exception as e:
                err = str(e)
                tb = format_exc()
                logger.error("Exception when running task: %s - %s" % (err, json.dumps(tb)))
                err = (err[252] + ' ...') if len(err) > 252 else err
                self.sfn.send_task_failure(taskToken=token, error=str(err), cause=tb)
