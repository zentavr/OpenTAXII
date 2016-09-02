import anyconfig
import argparse
import structlog

from opentaxii.entities import Collection, ServiceDefinition
from opentaxii.cli import app

log = structlog.getLogger(__name__)


def create_services():

    parser = argparse.ArgumentParser(
        description="Create services using OpenTAXII Persistence API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-c", "--services-config", dest="config",
        help="YAML file with services configuration", required=True)

    args = parser.parse_args()
    services_config = anyconfig.load(args.config, forced_type="yaml")

    with app.app_context():
        for service_params in services_config:

            if not 'type' in service_params:
                raise ValueError('No type specified for a service')

            service_id = service_params.pop('id', None)
            service_type = service_params.pop('type')

            existing = ServiceDefinition.get(service_id)

            if existing:
                log.warning(
                    "service.skipped.already_exists",
                    id=existing.id,
                    type=existing.__class__.__name__)
            else:
                service = ServiceDefinition(
                        id=service_id,
                        service_type=service_type,
                        properties=service_params)
                service = service.save()
                log.info("service.created",
                         id=service.id,
                         type=service.__class__.__name__)


def create_collections():

    parser = argparse.ArgumentParser(
        description="Create collections using OpenTAXII Persistence API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-c", "--collections-config", dest="config",
        help="YAML file with collections configuration", required=True)

    args = parser.parse_args()
    collections_config = anyconfig.load(args.config, forced_type="yaml")

    with app.app_context():

        created = 0
        for collection in collections_config:

            service_ids = collection.pop('service_ids')
            existing = None

            for service_id in service_ids:
                existing = Collection.get_by_name(
                    collection['name'],
                    service_id=service_id)
                if existing:
                    break

            if existing:
                log.warning(
                    "collection.skipped.already_exists",
                    collection=collection['name'],
                    existing_id=existing.id)
                continue

            c = Collection(**collection).save()

            app.managers.persistence.attach_collection_to_services(
                c.id, service_ids=service_ids)

            created += 1

            log.info("collection.created", collection=collection['name'])


def delete_content_blocks():

    parser = argparse.ArgumentParser(
        description=(
            "Delete content blocks from specified collections "
            "with timestamp labels matching defined time window"),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-c", "--collection", action="append", dest="collection",
        help="Collection to remove content blocks from", required=True)

    parser.add_argument(
        "--begin", dest="begin",
        help="exclusive beginning of time window as ISO8601 formatted date",
        required=True)

    parser.add_argument(
        "--end", dest="end",
        help="inclusive ending of time window as ISO8601 formatted date")

    args = parser.parse_args()

    with app.app_context():

        start_time = args.begin
        end_time = args.end

        for collection in args.collection:
            app.managers.persistence.delete_content_blocks(
                collection, start_time=start_time, end_time=end_time)
