import structlog

from ..exceptions import UnauthorizedException, InvalidAuthHeader
from ..local import context
from ..utils import parse_basic_auth_token

log = structlog.getLogger(__name__)

HTTP_AUTHORIZATION_HEADER = 'Authorization'


class AuthManager(object):
    '''Manager responsible for authentication.

    Manager uses API instance ``api`` for basic auth operations and
    provides additional logic on top.

    :param `opentaxii.auth.api.OpenTAXIIAuthAPI` api:
        instance of Auth API class
    '''

    def __init__(self, auth_api):
        self.api = auth_api

    def authenticate(self, username, password):
        '''Authenticate a user.

        :param str username: username
        :param str password: password

        :return: auth token
        :rtype: string
        '''
        return self.api.authenticate(username, password)

    def get_account(self, token):
        '''Get account for auth token.

        :param str token: auth token

        :return: an account entity
        :rtype: `opentaxii.entities.Account`
        '''
        return self.api.get_account(token)

    def create_account(self, username, password):
        '''Create an account.

        NOTE: Additional method that is only used in the helper scripts
        shipped with OpenTAXII.
        '''
        account = self.api.create_account(username, password)
        log.info("account.created", username=account.username)

        return account

    def is_basic_auth_supported(self):
        return context.config.get('support_basic_auth', False)

    def authenticate_request(self, headers):

        auth_header = headers.get(HTTP_AUTHORIZATION_HEADER)
        if not auth_header:
            return None

        parts = auth_header.split(' ', 1)

        if len(parts) != 2:
            log.warning('auth.header_invalid', value=auth_header)
            return None

        auth_type, raw_token = parts
        auth_type = auth_type.lower()

        if auth_type == 'basic':

            if not self.is_basic_auth_supported():
                raise UnauthorizedException()

            try:
                username, password = parse_basic_auth_token(raw_token)
            except InvalidAuthHeader:
                log.error("auth.basic_auth.header_invalid",
                          raw_token=raw_token, exc_info=True)
                return None

            token = self.authenticate(username, password)

        elif auth_type == 'bearer':
            token = raw_token
        else:
            raise UnauthorizedException()

        if not token:
            raise UnauthorizedException()

        account = self.get_account(token)

        if not account:
            raise UnauthorizedException()

        return account
