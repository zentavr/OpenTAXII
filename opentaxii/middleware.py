import structlog
import functools
import importlib
from flask import Flask, request, make_response, abort, current_app, g, send_from_directory

from .taxii1x.exceptions import (
    raise_failure, StatusMessageException, FailureStatus
)
from .taxii1x.utils import parse_message
from .taxii1x.status import process_status_exception
from .taxii1x.bindings import (
    MESSAGE_BINDINGS, SERVICE_BINDINGS, ALL_PROTOCOL_BINDINGS
)
from .taxii1x.http import (
    get_http_headers, get_content_type, validate_request_headers_post_parse,
    validate_request_headers, validate_response_headers,
    HTTP_X_TAXII_CONTENT_TYPE, HTTP_ALLOW
)
from .utils import parse_basic_auth_token, load_inner_api
from .local import release_context, context

from . import external_api

from .persistence import PersistenceManager
from .auth import AuthManager
from .entities import ServiceDefinition


log = structlog.get_logger(__name__)

from collections import namedtuple
Managers = namedtuple('Managers', ['auth', 'persistence'])


def create_app(config):
    '''
    :return: Flask app
    '''

    app = Flask(__name__, static_url_path='', static_folder='static')
    app.managers = init_managers(config, app)
    app.opentaxii_config = config

    external_api.init_app(app)

    # add catch-all rule for TAXII requests
    app.add_url_rule(
        "/taxii/<path:relative_path>", "taxii_endpoints",
        handle_taxii_request, methods=['GET', 'POST', 'OPTIONS'])

    app.add_url_rule(
        '/', 'index',
        lambda: app.send_static_file('index.html'),
        methods=['GET', 'OPTIONS'])

    # FIXME: tune accorting to request content-type
    app.register_error_handler(500, handle_internal_error)
    app.register_error_handler(StatusMessageException, handle_status_exception)

    app.before_request(
        functools.partial(authorize_and_prepare_context, app))
    app.teardown_request(release_context_after_request)

    setup_hooks(config)

    # Populate custom local context, because using Flask local proxy
    # is not possible with sophisticated inner API integrations
    populate_context(app)

    return app


def setup_hooks(config):
    signal_hooks = config['hooks']
    if signal_hooks:
        importlib.import_module(signal_hooks)
        log.info("signal_hooks.imported", hooks=signal_hooks)


def init_managers(config, app):
    persistence_manager = PersistenceManager(
        load_inner_api(config['persistence_api']))

    auth_manager = AuthManager(
        load_inner_api(config['auth_api']))

    persistence_manager.api.init_app(app)
    auth_manager.api.init_app(app)

    managers = Managers(
        auth=auth_manager,
        persistence=persistence_manager)

    return managers


def populate_context(app):
    context.config = app.opentaxii_config
    context.managers = app.managers


def authorize_and_prepare_context(app):
    context.account = app.managers.auth.authenticate_request(request.headers)
    populate_context(app)


def release_context_after_request(exception=None):
    release_context()


def handle_taxii_request(relative_path=''):
    relative_path = '/' + relative_path

    if is_possibly_taxii1x_request(request):

        for service in ServiceDefinition.get_all():
            if service.path == relative_path:

                if (service.authentication_required and
                        context.account is None):
                    raise UnauthorizedException()

                if not service.available:
                    raise_failure("The service is not available")

                if request.method == 'POST':
                    return _process_with_service(service)
                elif request.method == 'OPTIONS':
                    return _process_options_request(service)
        abort(404)
    elif is_possibly_taxii2x_request(request):
        # Placeholder for TAXII2.0 support
        abort(415)
    else:
        abort(404)


def is_possibly_taxii1x_request(request):
    return (
        'application/xml' in request.accept_mimetypes
        and HTTP_X_TAXII_CONTENT_TYPE in request.headers)


def is_possibly_taxii2x_request(request):
    # TODO: extend
    return ('application/json' in request.accept_mimetypes)


def _process_with_service(service):

    if 'application/xml' not in request.accept_mimetypes:
        raise_failure(
            "The specified values of Accept is not supported: {}"
            .format(", ".join((request.accept_mimetypes or []))))

    validate_request_headers(request.headers, MESSAGE_BINDINGS)

    taxii_message = parse_message(
        get_content_type(request.headers), request.data)

    try:
        validate_request_headers_post_parse(
            request.headers,
            supported_message_bindings=MESSAGE_BINDINGS,
            service_bindings=SERVICE_BINDINGS,
            protocol_bindings=ALL_PROTOCOL_BINDINGS)
    except StatusMessageException as e:
        e.in_response_to = taxii_message.message_id
        raise e

    response_message = service.process(request.headers, taxii_message)

    response_headers = get_http_headers(
        response_message.version, request.is_secure)
    validate_response_headers(response_headers)

    # FIXME: pretty-printing should be configurable
    taxii_xml = response_message.to_xml(pretty_print=True)

    return make_taxii_response(taxii_xml, response_headers)


def _process_options_request(service):

    message_bindings = ','.join(service.supported_message_bindings or [])

    return "", 200, {
        HTTP_ALLOW: 'POST, OPTIONS',
        HTTP_X_TAXII_CONTENT_TYPES: message_bindings
    }


def make_taxii_response(taxii_xml, taxii_headers):

    validate_response_headers(taxii_headers)
    response = make_response(taxii_xml)

    h = response.headers
    for header, value in taxii_headers.items():
        h[header] = value

    return response


def handle_status_exception(error):
    log.warning('Status exception', exc_info=True)

    if 'application/xml' not in request.accept_mimetypes:
        return 'Unacceptable', 406

    xml, headers = process_status_exception(
        error, request.headers, request.is_secure)
    return make_taxii_response(xml, headers)


def handle_internal_error(error):
    log.error('Internal error', exc_info=True)

    if 'application/xml' not in request.accept_mimetypes:
        return 'Unacceptable', 406

    new_error = FailureStatus("Error occured", e=error)

    xml, headers = process_status_exception(
        new_error, request.headers, request.is_secure)
    return make_taxii_response(xml, headers)
