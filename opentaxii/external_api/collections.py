
from marshmallow import Schema, fields, post_load, pre_load
from marshmallow.validate import OneOf

from ..entities import Collection
from ..utils import is_content_supported
from .helpers import ResourceView


def init_app(app, url_prefix=None):
    CollectionView.register(app, route_prefix=url_prefix)


class ContentBindingSchema(Schema):
    binding = fields.String(required=True)
    subtypes = fields.List(fields.String(), allow_none=True)


class CollectionSchema(Schema):

    id = fields.Integer()

    name = fields.String(required=True)
    description = fields.String()

    type = fields.String(validate=OneOf([
        Collection.TYPE_FEED, Collection.TYPE_SET]))

    available = fields.Boolean(default=True)
    volume = fields.Integer()
    accept_all_content = fields.Boolean()

    supported_content_bindings = fields.Nested(ContentBindingSchema, many=True)

    @pre_load
    def normalize_content_bindings(self, data):
        if 'supported_content_bindings' in data:
            bindings = data['supported_content_bindings']
            normalized = []

            for binding in bindings:
                if isinstance(binding, six.string_types):
                    normalized.append({'binding': binding})
                elif isinstance(binding, (list, tuple)) and len(binding) > 1:
                    normalized.append({
                        'binding': binding[0],
                        'subtypes': binding[1]
                    })
                else:
                    # Let marshmallow continue with validation
                    normalized.append(binding)

            data['supported_content_bindings'] = normalized
            return data


class CollectionView(ResourceView):
    route_base = 'collections'
    route_prefix = '/api'
    schema_class = CollectionSchema
    resource_class = Collection
