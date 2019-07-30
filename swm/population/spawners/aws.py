import json
import subprocess

import botocore.exceptions
import botocore.session

from . import BaseSpawner


class LambdaSpawner(BaseSpawner):
    """
    Spawn a new worker on the local system.
    """

    def __init__(self, function_name, payload, client_kwargs=None):

        # The function name to invoke
        self._function_name = function_name

        # The payload to send when invoking the function
        self._payload = payload

        # The keyword arguments to when initializing the lambda client
        self._client_kwargs = client_kwargs or {}


    def spawn(self, amount):

        client = botocore.session.get_session().create_client(
            'lambda',
            **client_kwargs
        )

        for w in range(0, amount):
            client.invoke(
                FunctionName=self._function_name,
                InvocationType='Event',
                Payload=json.dumps(self._payload)
            )
