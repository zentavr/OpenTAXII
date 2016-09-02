
from flask import request, jsonify, abort, current_app, Blueprint
from collections import defaultdict

from ..local import context

from . import collections, services

external_api = Blueprint('external_api', __name__, static_folder=None)


def init_app(app):

    app.register_blueprint(external_api, url_prefix='/api')

    collections.init_app(app, url_prefix='/api')
    services.init_app(app, url_prefix='/api')

    # app.register_error_handler(500, handle_internal_error)


def some():
    pass
    # JSON based error handler


@external_api.route('/auth')
def auth():
    data = request.get_json() or request.form

    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return 'Both username and password are required', 400

    token = context.managers.auth.authenticate(username, password)

    if not token:
        abort(401)

    return jsonify(token=token.decode('utf-8'))


@external_api.route('/')
def list_urls():
    rules = defaultdict(set)
    for r in current_app.url_map.iter_rules():
        rules[r.rule].update(r.methods)

    rules = dict(sorted((r, sorted(list(m)))
                        for r, m in rules.items()))
    return jsonify({"data": rules})


@external_api.route('/health')
def health():
    return jsonify(alive=True)
