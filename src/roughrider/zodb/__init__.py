import transaction
from contextlib import contextmanager
from zodburi import resolve_uri
from ZODB import DB
from ZODB.ActivityMonitor import ActivityMonitor


class ZODB:

    databases: dict

    def __init__(self, zodb_config):
        self.databases = {}
        for name, uri in zodb_config.items():
            db = self.db_from_uri(uri, name, self.databases)
            db.setActivityMonitor(ActivityMonitor())

    @staticmethod
    def db_from_uri(uri: str, name: str, databases: dict):
        factory, params = resolve_uri(uri)
        params['database_name'] = name
        storage = factory()
        return DB(storage, databases=databases, **params)

    @contextmanager
    def database(self, name, transaction_manager=None):
        db = self.databases[name]
        conn = db.open(transaction_manager)
        try:
            yield conn
        finally:
            if not conn.transaction_manager.isDoomed():
                conn.transaction_manager.commit()
            conn.close()

    def middleware(self, app):

        def zodb_application(environ, start_response):
            with transaction.manager as tm:
                environ['transaction.manager'] = tm
                environ['ZODB'] = self
                response = app(environ, start_response)
            return response

        return zodb_application
