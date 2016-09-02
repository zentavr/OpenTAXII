import structlog
import furl

from libtaxii.common import generate_message_id
from libtaxii.constants import (
    VID_TAXII_XML_10, VID_TAXII_XML_11,
    VID_TAXII_HTTP_10, VID_TAXII_HTTPS_10
)

from ...local import context
from ..exceptions import StatusMessageException, raise_failure
from ..converters import service_to_service_instances


PROTOCOL_TO_SCHEME = {
    VID_TAXII_HTTP_10: 'http',
    VID_TAXII_HTTPS_10: 'https'
}


class TAXIIService(object):
    '''Generic TAXII Service class.

    This class implements common methods for all TAXII services.

    :param str id: service ID
    :param str address: service address as absolute URL
    :param str description: service description

    :param list protocol_bindings: list of supported protocol bindings
        as a list of strings
    :param bool available: if the service is available
    :param bool authentication_required: if authentication required
    '''

    id = None
    description = 'Default TAXII service description'
    service_type = None

    available = True

    authentication_required = False

    supported_message_bindings = [VID_TAXII_XML_10, VID_TAXII_XML_11]
    supported_protocol_bindings = ()

    def __init__(self, id, address, description=None,
                 protocol_bindings=None, available=True,
                 authentication_required=False,
                 properties=None):

        self.id = id
        self.address = add_domain_to_address(address)
        self.path = str(furl.furl(address).path)

        self.description = description
        self.supported_protocol_bindings = (
            protocol_bindings or self.supported_protocol_bindings)

        self.available = available
        self.authentication_required = authentication_required

        self.properties = properties

        self.log = structlog.getLogger(
            "{}.{}".format(self.__module__, self.__class__.__name__),
            service_id=id)

        if not self.supported_protocol_bindings:
            self.log.warning(
                "No protocol bindings specified, service will be invisible",
                service=self.id)

    def generate_id(self):
        return generate_message_id()

    def process(self, headers, message):

        self.log.debug(
            "Processing message",
            message_id=message.message_id,
            message_type=message.message_type,
            message_version=message.version)

        handler = self.get_message_handler(message)

        handler.validate_headers(headers, in_response_to=message.message_id)
        handler.verify_message_is_supported(message)

        try:
            response_message = handler.handle_message(self, message)
        except StatusMessageException:
            raise
        except Exception:
            raise_failure(
                "There was a failure while executing the message handler",
                in_response_to=message.message_id)

        if not response_message:
            raise_failure(
                "The message handler {} did not return a TAXII Message"
                .format(handler),
                in_response_to=message.message_id)

        return response_message

    def get_message_handler(self, message):
        try:
            return self.handlers[message.message_type]
        except KeyError:
            self.log.warning(
                "Message not supported",
                message_id=message.message_id,
                message_type=message.message_type,
                message_version=message.version)
            raise_failure(
                "Message not supported by this service",
                in_response_to=message.message_id)

    def to_service_instances(self, version):
        return service_to_service_instances(self, version)

    def get_absolute_address(self, binding):
        address = self.address

        if binding in PROTOCOL_TO_SCHEME:
            scheme = PROTOCOL_TO_SCHEME[binding]
            f = furl.furl(address)
            if scheme and scheme != f.scheme:
                f.scheme = scheme
            address = f.url
        else:
            self.log.warning("binding.not_recognized",
                             binding=binding, address=address)

        return address

    def __repr__(self):
        return (
            "{}(id={}, address={})"
            .format(self.__class__.__name__, self.id, self.address))


def add_domain_to_address(address):

    domain = context.managers.persistence.get_domain()

    f = furl.furl(address)
    # clean up scheme and port because bindings have their own schemes
    f.scheme = None
    f.port = None
    if not f.host:
        f.host = domain
    return f.url
