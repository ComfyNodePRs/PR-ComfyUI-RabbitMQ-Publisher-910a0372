import base64
from io import BytesIO

import numpy as np
from PIL import Image

from server import PromptServer
from ..mq_client import RabbitMQClient


class PublishImageToRabbitMQ:
    @classmethod
    def INPUT_TYPES(s):
        return {
            'required': {
                'exchange': ('STRING', {'multiline': False, 'default': 'comfy'}),
                'routing_key': ('STRING', {'multiline': False, 'default': 'image'}),
                'host': ('STRING', {'multiline': False, 'default': 'localhost'}),
                'port': ('INT', {'default': 5672}),
                'username': ('STRING', {'multiline': False, 'default': 'guest'}),
                'password': ('STRING', {'multiline': False, 'default': 'guest'}),
                'exchange_type': ('STRING', {
                    'multiline': False,
                    'default': 'direct',
                    'options': ['fanout', 'direct', 'topic']  # TODO support header type
                }),
                'durable': ('BOOLEAN', {'default': True}),
                'auto_delete': ('BOOLEAN', {'default': False}),
                'internal': ('BOOLEAN', {'default': False}),
                'images': ('IMAGE',),
            },
            "hidden": {"prompt": "PROMPT", "extra_pnginfo": "EXTRA_PNGINFO"},
        }

    RETURN_TYPES = ('STRING',)
    # RETURN_NAMES = ()
    FUNCTION = 'publish_images'
    OUTPUT_NODE = True
    OUTPUT_IS_LIST = (True,)
    CATEGORY = 'ComfyRabbitMQ'

    def publish_images(
            self,
            exchange,
            routing_key,
            host,
            port,
            username,
            password,
            exchange_type,
            durable,
            auto_delete,
            internal,
            images,
            prompt=None,
            extra_pnginfo=None
    ):
        prompt_id = PromptServer.instance.last_prompt_id  # get current prompt id

        results = []
        for (batch_number, image) in enumerate(images):
            i = 255. * image.cpu().numpy()
            img = Image.fromarray(np.clip(i, 0, 255).astype(np.uint8))
            img_byte_array = BytesIO()
            img.save(img_byte_array, format='PNG')
            img_data = img_byte_array.getvalue()
            img_base64 = base64.b64encode(img_data).decode('utf-8')

            results.append({
                'batch_number': batch_number,
                'base64_data': img_base64,
            })

        mq_client = RabbitMQClient(exchange, routing_key, host, port, username, password)
        mq_client.declare_exchange(exchange_type, durable, auto_delete, internal)
        mq_client.publish({
            'images': results,
            'prompt_id': prompt_id,
        })
        mq_client.close()

        return { "ui": { "images": prompt_id } }

