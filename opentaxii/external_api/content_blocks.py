import six

from marshmallow import Schema, fields, post_load, pre_load
from marshmallow.validate import OneOf

from ..entities import ContentBlock
from ..utils import is_content_supported
from .helpers import ResourceView

from .collections import ContentBindingSchema


def init_app(app, url_prefix=None):
    ContentBlockView.register(app, route_prefix=url_prefix)


class ContentBlockSchema(Schema):

    id = fields.String()

    content = fields.String()
    content_binding = fields.Nested(ContentBindingSchema)

    timestamp_label = fields.DateTime()


class ContentBlockView(ResourceView):
    route_base = 'content-blocks'
    schema_class = ContentBlockSchema
    resource_class = ContentBlock
