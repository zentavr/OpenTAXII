from opentaxii.config import load_configuration
from opentaxii.middleware import create_app
from opentaxii.utils import configure_logging

config = load_configuration()
configure_logging(
    config.get('logging', {'': 'info'}),
    plain=True)

app = create_app(config)
app.debug = True
