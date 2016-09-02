
from .taxii1x.exceptions import UnauthorizedStatus


class InvalidAuthHeader(Exception):
    pass


class UnauthorizedException(UnauthorizedStatus):
    pass
