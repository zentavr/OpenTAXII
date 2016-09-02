import pytz
import structlog
from datetime import datetime

from lxml.etree import XMLSyntaxError

from .exceptions import BadMessageStatus
from .bindings import MESSAGE_VALIDATOR_PARSER

log = structlog.getLogger(__name__)


def parse_message(content_type, body, do_validate=True):

    validator_parser = MESSAGE_VALIDATOR_PARSER[content_type]

    if do_validate:
        try:
            result = validator_parser.validator.validate_string(body)
            if not result.valid:
                errors = '; '.join([str(err) for err in result.error_log])
                raise BadMessageStatus(
                    'Request was not schema valid: {}'
                    .format(errors))
        except XMLSyntaxError as e:
            log.error("Invalid XML received", exc_info=True)
            raise BadMessageStatus('Request was invalid XML', e=e)

    taxii_message = validator_parser.parser(body)

    return taxii_message
