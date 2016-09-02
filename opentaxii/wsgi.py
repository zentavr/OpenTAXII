
from .middleware import create_app
from .config import load_configuration
from .server import TAXIIServer
from .utils import configure_logging

config = load_configuration()
configure_logging(config.get('logging', {'': 'info'}))

app = create_app(config)
app.debug = False
