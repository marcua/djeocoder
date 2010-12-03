from settings_loader import get_settings
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import ArgumentError, InterfaceError, SAWarning

import warnings

settings = get_settings()

first_dbtables = None

class DbTables():
    """
        Create a DbTables instance to connect to the database specified in settings.py
        The object will present sqlalchemy tables as properties:
            - blocks
            - intersections
    """
    def __init__(self):
        global first_dbtables
        if first_dbtables == None:
            try:
                dburi = settings.DATABASE_URI
                dbconfig = None
                try:
                    dbconfig = settings.DATABASE_CONFIG
                except AttributeError:
                    pass
                if dbconfig == None:
                    self.engine = create_engine(dburi, echo=False, pool_size=1)
                else:
                    self.engine = create_engine(dburi, connect_args=dbconfig, echo=False, pool_size=1)
                self.meta = MetaData()
                warnings.filterwarnings('ignore', category = SAWarning)
                self.meta.reflect(bind=self.engine)
                self.blocks = self.meta.tables['blocks']
                self.intersections = self.meta.tables['intersections']
                first_dbtables = self
            except AttributeError, e:
                raise e
                raise SettingsException("To put results INTO a TABLE, please specify a DATABASE_URI in settings.py")
            except ArgumentError, e:
                raise DbException(e)
        else:
            self.__dict__ = first_dbtables.__dict__

