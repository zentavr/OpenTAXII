import structlog
from opentaxii.signals import (
    CONTENT_BLOCK_CREATED, INBOX_MESSAGE_CREATED,
    SUBSCRIPTION_CREATED
)

from ..entities import (
    Collection, ContentBlock, Subscription, ResultSet,
    ServiceDefinition)

log = structlog.getLogger(__name__)


class PersistenceManager(object):
    '''Manager responsible for persisting and retrieving data.

    Manager uses API instance ``api`` for basic data CRUD operations and
    provides additional logic on top.

    :param `opentaxii.persistence.api.OpenTAXIIPersistenceAPI` api:
        instance of persistence API class
    '''

    def __init__(self, persistence_api):
        self.api = persistence_api

    def save_resource(self, resource):
        if isinstance(resource, Collection):
            obj = self.api.save_collection(resource)
        elif isinstance(resource, ServiceDefinition):
            obj = self.api.save_service(resource)
        elif isinstance(resource, ContentBlock):
            obj = self.api.save_content_block(resource)
        elif isinstance(resource, Subscription):
            obj = self.api.save_subscription(resource)
        elif isinstance(resource, ResultSet):
            obj = self.api.save_result_set(resource)
        else:
            raise ValueError(
                'Unknown resource type "{}"'.format(type(resource)))

        return obj

    def get_resource(self, resource_class, id, **params):

        if resource_class == Collection:
            obj = self.api.get_collection(id, **params)
        elif resource_class == ServiceDefinition:
            obj = self.api.get_service(id, **params)
        elif resource_class == ContentBlock:
            obj = self.api.get_content_block(id, **params)
        elif resource_class == Subscription:
            obj = self.api.get_subscription(id, **params)
        elif resource_class == ResultSet:
            obj = self.api.get_result_set(id, **params)
        else:
            raise ValueError(
                'Unknown resource type "{}"'.format(resource_class.__name__))
        return obj

    def get_resources(self, resource_class, **params):
        if resource_class == Collection:
            objs = self.api.get_collections(**params)
        elif resource_class == ServiceDefinition:
            objs = self.api.get_services(**params)
        elif resource_class == ContentBlock:
            objs = self.api.get_content_blocks(**params)
        elif resource_class == Subscription:
            objs = self.api.get_subscriptions(**params)
        elif resource_class == ResultSet:
            objs = self.api.get_result_sets(**params)
        return objs

    def attach_collection_to_services(self, collection_id, service_ids):
        '''Attach collection to the services.

        NOTE: Additional method that is only used in the helper scripts
        shipped with OpenTAXII.
        '''
        return self.api.attach_collection_to_services(
            collection_id, service_ids)

    def get_collection_by_name(self, name, service_id=None):
        return self.api.get_collection_by_name(
            name, service_id=service_id)

    def get_services_for_collection(self, collection):
        '''Get the services associated with a collection.

        :param `opentaxii.taxii.entities.CollectionEntity` collection:
            collection entity in question

        :return: list of service entities.
        :rtype: list of :py:class:`opentaxii.taxii.entities.ServiceEntity`
        '''

        return self.api.get_services(collection_id=collection.id)

    def get_content_blocks_count(self, collection_id, start_time=None,
                                 end_time=None, bindings=None):
        '''Get a count of the content blocks associated with a collection.

        :param str collection_id: ID fo a collection in question
        :param datetime start_time: start of a time frame
        :param datetime end_time: end of a time frame
        :param list bindings: list of
            :py:class:`opentaxii.taxii.entities.ContentBindingEntity`

        :return: content block count
        :rtype: int
        '''

        return self.api.get_content_blocks_count(
            collection_id=collection_id,
            start_time=start_time,
            end_time=end_time,
            bindings=bindings or [],
        )

    def get_content_blocks(self, collection_id, start_time=None, end_time=None,
                           bindings=None, offset=0, limit=None):
        '''Get the content blocks associated with a collection.

        :param str collection_id: ID fo a collection in question
        :param datetime start_time: start of a time frame
        :param datetime end_time: end of a time frame
        :param list bindings: list of
            :py:class:`opentaxii.taxii.entities.ContentBindingEntity`
        :param int offset: result set offset
        :param int limit: result set max size

        :return: content blocks list
        :rtype: list of :py:class:`opentaxii.taxii.entities.ContentBlockEntity`
        '''

        return self.api.get_content_blocks(
            collection_id=collection_id,
            start_time=start_time,
            end_time=end_time,
            bindings=bindings or [],
            offset=offset,
            limit=limit,
        )

    def get_domain(self):
        '''Get configured domain name needed to create absolute URLs.
        '''
        return self.api.get_domain()

    def delete_content_blocks(self, collection_name, start_time,
                              end_time=None):
        '''Delete content blocks in a specified collection with
        timestamp label in a specified time frame.

        :param str collection_name: collection name
        :param datetime start_time: exclusive beginning of a timeframe
        :param datetime end_time: inclusive end of a timeframe

        :return: the count of rows deleted
        :rtype: int
        '''
        count = self.api.delete_content_blocks(
            collection_name, start_time, end_time=end_time)

        log.info(
            "collection.content_blocks.deleted",
            collection=collection_name,
            count=count)

        return count
