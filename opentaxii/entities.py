import six

from .utils import is_content_supported

from .local import context


from libtaxii.constants import (
    SS_ACTIVE, SS_PAUSED, SS_UNSUBSCRIBED,
    RT_FULL, RT_COUNT_ONLY
)


def init_app(app):
    pass


class Account(object):
    '''Represents Account entity.

    This class holds user-specific information and is used
    for authorization.

    :param str id: account id
    :param dict details: additional details of an account
    '''

    def __init__(self, id, username, **details):

        self.id = id
        self.username = username
        self.details = details


class Resource(object):

    def save(self):
        return context.managers.persistence.save_resource(self)

    @classmethod
    def get(cls, id):
        return context.managers.persistence.get_resource(cls, id)

    @classmethod
    def get_all(cls, **kwargs):
        return context.managers.persistence.get_resources(cls, **kwargs)

    def __repr__(self):
        pairs = ["%s=%s" % (k, v) for k, v in sorted(self.__dict__.items())]
        return "%s(%s)" % (self.__class__.__name__, ", ".join(pairs))


class ContentBlock(Resource):

    def __init__(self, content, timestamp_label, content_binding,
                 collection_ids=None, id=None, message=None,
                 inbox_message_id=None):

        self.content = content

        self.id = id
        self.timestamp_label = timestamp_label
        self.content_binding = content_binding
        self.message = message
        self.inbox_message_id = inbox_message_id
        self.collection_ids = collection_ids


class Collection(Resource):

    TYPE_FEED = 'DATA_FEED'
    TYPE_SET = 'DATA_SET'

    def __init__(self, name, id=None, description=None, type=TYPE_FEED,
                 volume=None, accept_all_content=False,
                 supported_content_bindings=None, available=True):

        self.id = id
        self.name = name
        self.volume = volume
        self.description = description
        self.accept_all_content = accept_all_content
        self.type = type
        self.supported_content_bindings = convert_to_content_bindings_resource(
            supported_content_bindings or [])
        self.available = available

    def is_content_supported(self, content_binding):
        if self.accept_all_content:
            return True

        return is_content_supported(
            self.supported_content_bindigns,
            content_binding)

    def get_matching_bindings(self, requested_bindings):
        if self.accept_all_content:
            return requested_bindings

        if not self.supported_content_bindings:
            return requested_bindings

        if not requested_bindings:
            return self.supported_content_bindings

        overlap = []

        for requested in requested_bindings:
            for supported in self.supported_content_bindings:

                if requested.binding != supported.binding:
                    continue

                if not supported.subtypes:
                    overlap.append(requested)
                    continue

                if not requested.subtypes:
                    overlap.append(supported)
                    continue

                subtypes_overlap = (
                    set(supported.subtypes).intersection(requested.subtypes))

                overlap.append(ContentBinding(
                    binding=requested.binding,
                    subtypes=subtypes_overlap
                ))

        return overlap

    @classmethod
    def get_by_name(cls, name, service_id=None):
        return context.managers.persistence.get_collection_by_name(
            name, service_id=service_id)

    def __repr__(self):
        return (
            "CollectionEntity(name={}, type={}, supported_content_bindings={})"
            .format(self.name, self.type, self.supported_content_bindings))


class ContentBinding(Resource):
    '''TAXII Content Binding entity.

    :param str binding: content binding ID
    :param list subtypes: list of subtype ids
    '''

    def __init__(self, binding, subtypes=None):
        self.binding = binding
        self.subtypes = subtypes or []


class ServiceDefinition(Resource):
    '''TAXII Service entity.

    :param str type: service type,
        supported values are listed as keys in
        :py:attr:`opentaxii.server.TAXIIServer.TYPE_TO_SERVICE`
    :param dict properties: a dictionary with service-specific properties
    :param str id: service ID
    '''

    TYPES = ['inbox', 'discovery', 'collection_management', 'poll']

    def __init__(self, service_type, properties, id=None):
        self.id = id
        self.service_type = service_type
        self.properties = properties


class InboxMessage(Resource):
    '''TAXII Inbox Message Resource

    :param str message_id: TAXII message ID
    :param str original_message: XML serialized original TAXII message
    :param int content_block_count:
        how many content blocks this message contains
    :param str service_id: ID of the Inbox Service that received the message

    :param str id: internal ID of the inbox message entity
    :param str result_id:
        ID of the Result Set part of which this message delivers
    :param list destination_collections:
        a list of destination collections, as a list of strings

    :param int record_count:
        how many items left in the Result Set this message is part of
    :param bool partial_count: if the record count is partial

    :param str subscription_id: ID of a subscription
    :param str subscription_collection_name:
        collection name of the subscription

    :param datetime exclusive_begin_timestamp_label:
        subscription's exclusive begin timestamp label
    :param datetime inclusive_begin_timestamp_label:
        subscription's inclusive begin timestamp label
    '''

    def __init__(self, message_id, original_message, content_block_count,
                 service_id, id=None, result_id=None,
                 destination_collections=None, record_count=None,
                 partial_count=False, subscription_collection_name=None,
                 subscription_id=None, exclusive_begin_timestamp_label=None,
                 inclusive_end_timestamp_label=None):

        self.id = id

        self.message_id = message_id
        self.original_message = original_message
        self.content_block_count = content_block_count

        self.service_id = service_id

        self.destination_collections = destination_collections or []

        self.result_id = result_id
        self.record_count = record_count
        self.partial_count = partial_count

        self.subscription_collection_name = subscription_collection_name
        self.subscription_id = subscription_id

        self.exclusive_begin_timestamp_label = exclusive_begin_timestamp_label
        self.inclusive_end_timestamp_label = inclusive_end_timestamp_label


class ResultSet(Resource):
    '''TAXII Result Set entity.

    :param str id: ID of a Result Set
    :param str collection_id: ID of a collection
    :param list content_bindings:
        list of :class:`ContentBindingResource` instances
    :param tuple timeframe:
        a timeframe of the Result Set in a form of ``(begin, end)``
    '''

    def __init__(self, id, collection_id, content_bindings=None,
                 timeframe=None):

        self.id = id

        self.collection_id = collection_id
        self.content_bindings = content_bindings or []
        self.timeframe = timeframe or (None, None)


class SubscriptionParameters(Resource):
    '''TAXII Subscription Parameters entity.

    Note: query formats specification is not supported

    :param str response_type: response type, supported values
        are :attr:`FULL` and :attr:`COUNT_ONLY`
    :param list content_bindings:
        list of :class:`ContentBindingResource` instances
    '''

    FULL = RT_FULL
    COUNT_ONLY = RT_COUNT_ONLY

    def __init__(self, response_type=FULL, content_bindings=None):

        self.response_type = response_type
        self.content_bindings = content_bindings or []


class PollRequestParameters(SubscriptionParameters):
    '''TAXII Poll Request Parameters entity.

    Note: allow_asynch and delivery_parameters fields are not supported

    :param str response_type: response type, supported values
        are :attr:`FULL` and :attr:`COUNT_ONLY`
    :param list content_bindings:
        list of :class:`ContentBindingResource` instances
    '''

    def __init__(self, response_type=SubscriptionParameters.FULL,
                 content_bindings=None):

        super(PollRequestParameters, self).__init__(
            response_type=response_type, content_bindings=content_bindings)


class Subscription(Resource):
    '''TAXII Subscription entity.

    :param str service_id: ID of a service
    :param str collection_id: ID of a collection
    :param str subscription_id: ID of a subscription
    :param str status: subscription status, supported values are:
        :attr:`ACTIVE`, :attr:`PAUSED`, :attr:`UNSUBSCRIBED`
    :param `PollRequestParametersResource` poll_request_params:
        Poll Request Parameters entity
    '''

    ACTIVE = SS_ACTIVE
    PAUSED = SS_PAUSED
    UNSUBSCRIBED = SS_UNSUBSCRIBED

    def __init__(self, service_id, collection_id, subscription_id=None,
                 status=ACTIVE, poll_request_params=None):

        self.service_id = service_id
        self.collection_id = collection_id
        self.subscription_id = subscription_id
        self.params = poll_request_params
        self.status = status


def convert_to_content_bindings_resource(content_bindings):

    normalized = []

    for binding in content_bindings:
        if isinstance(binding, six.string_types):
            binding = ContentBinding(binding)
        elif isinstance(binding, (list, tuple)) and len(binding) > 1:
            binding = ContentBinding(
                binding=binding[0],
                subtypes=binding[1]
            )
        elif isinstance(binding, dict):
            binding = ContentBinding(**binding)
        elif isinstance(binding, ContentBinding):
            # Nothing to do, binding is alerady of a correct type
            pass
        else:
            raise ValueError('Unknown binding type "{}"'.format(binding))
        normalized.append(binding)
    return normalized
