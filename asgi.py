from dotenv import load_dotenv

load_dotenv()

from lumen import create_app
from lumen.services.db_pool import resolve_wsgi_workers
from lumen.services.wsgi_disconnect import DisconnectAwareWSGIMiddleware

flask_app = create_app()

app = DisconnectAwareWSGIMiddleware(
    flask_app,
    workers=resolve_wsgi_workers(flask_app.config.get("SQLALCHEMY_ENGINE_OPTIONS", {})),
)
