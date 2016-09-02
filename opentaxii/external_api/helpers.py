from flask import request, jsonify, abort
from flask_classy import FlaskView

from werkzeug.exceptions import HTTPException as WerkzeugException

import marshmallow

from ..local import context


class JSONView(FlaskView):

    schema_class = None

    def __init__(self):
        # According to JSON-API, resources must specify a 'type' field that
        # matches the endpoint name, so we add it as an extra schema field:
        self.schema = self.schema_class(extra={'type': self.route_base})

    def _schema_load_json(self):
        return load_json(self.schema)

    def _schema_load_json_strict(self):
        return load_json_strict(self.schema)

    def _schema_load_json_ignore_missing(self):
        return load_json_ignore_missing(self.schema)

    def _single_object_response(self, obj):
        return single_object_response(self.schema, obj)

    def _many_objects_response(self, objs):
        return many_objects_response(self.schema, objs)

    def _new_object_response(self, obj):
        return new_object_response(self.schema, obj)


class ResourceView(JSONView):

    resource_class = None

    def index(self):
        objs = self.resource_class.get_all()
        return self._many_objects_response(objs)

    def get(self, id):
        resource = self.resource_class.get(id)
        if not resource:
            abort(404)
        return self._single_object_response(resource)

    def post(self):
        data = self._schema_load_json_strict()
        obj = self._process_post(data)
        return self._new_object_response(obj)

    def _process_post(self, data):
        obj = self.resource_class(**data)
        return self._save_new_object(obj)

    def _save_new_object(self, obj):
        obj = context.managers.persistence.save_resource(obj)
        return obj

    def put(self, id):
        obj = self.resource_class.get(id)
        if not obj:
            abort(404)
        data = self._schema_load_json_ignore_missing()
        obj = self._process_put(obj, data)
        return self._single_object_response(obj)

    def _process_put(self, obj, data):
        for key, value in data.items():
            if key == 'id':
                continue
            setattr(obj, key, value)

        obj = context.managers.persistence.save_resource(obj)
        return obj

    def delete(self, id):
        self._process_delete(self.resource_class, id)
        # FIXME: really?
        return '', 204

    def _process_delete(self, resource_class, id):
        context.managers.persistence.remove_resource(resource_class, id)


def load_json(schema):
    body = request.get_json(force=True)
    if not isinstance(body, dict):
        raise HTTPException(400, "Request body is not a JSON object.")
    json_data = body.get('data')
    if json_data is None:
        raise HTTPException(400, "Data field is missing.")
    elif not isinstance(json_data, dict):
        raise HTTPException(400, "Data field is not a JSON object.")
    return schema.load(json_data)


def load_json_strict(schema):
    """Loads json request data and raises an exception if validation fails.
    """
    result = load_json(schema)
    if result.errors:
        raise_validation_errors(result.errors)
    return result.data


def load_json_ignore_missing(schema):
    """Loads json request data, but ignores missing fields.
    Only performs validation on those fields that are present.
    """
    result = load_json(schema)

    json_data = request.get_json(force=True)['data']

    relevant_errors = {}
    for field in list(json_data.keys()) + [marshmallow.marshalling.SCHEMA]:
        if field in result.errors:
            relevant_errors[field] = result.errors[field]

    if relevant_errors:
        raise_validation_errors(relevant_errors)

    return result.data


def single_object_response(schema, obj):
    data = schema.dump(obj).data
    return jsonify({'data': data})


def many_objects_response(schema, objs):
    data = schema.dump(objs, many=True).data
    return jsonify({'data': data})


def new_object_response(schema, obj):
    return_data = {'data': schema.dump(obj).data}
    return jsonify(return_data), 201


class HTTPException(WerkzeugException):

    def __init__(self, code=None, description=None, json_errors=None):
        super(HTTPException, self).__init__()
        if code is not None:
            self.code = code
        if description is not None:
            self.description = description

        if json_errors:
            self.json_errors = json_errors


def _build_details(errors):
    # FIXME
    pass


def raise_validation_errors(errors):
    validation_errors = []
    if isinstance(errors, dict):
        for field, detail in errors.items():
            json_error = {'title': 'Validation failed',
                          'field': field,
                          'detail': _build_details(detail)}
            validation_errors.append(json_error)
    else:
        validation_errors.append({
            'title': 'Validation failed',
            'detail': _build_details(errors)
        })

    raise HTTPException(400, json_errors=validation_errors)
