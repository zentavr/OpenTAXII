import six

from marshmallow import Schema, fields, post_load, pre_load
from marshmallow.validate import OneOf

from ..entities import ServiceDefinition
from ..utils import is_content_supported
from .helpers import ResourceView


def init_app(app, url_prefix=None):
    ServiceView.register(app, route_prefix=url_prefix)


class ServiceDefinitionSchema(Schema):

    id = fields.String()

    service_type = fields.String(
        allow_none=False, required=True,
        validate=OneOf(ServiceDefinition.TYPES))

    properties = fields.Dict()


class ServiceView(ResourceView):
    route_base = 'services'
    schema_class = ServiceDefinitionSchema
    resource_class = ServiceDefinition
