"""Database connection and management"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from config.config import Config

Base = declarative_base()

class Database:
    """Database connection manager"""
    
    _instance = None
    _engine = None
    _SessionLocal = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._engine is None:
            self._init_engine()
    
    def _init_engine(self):
        """Initialize database engine"""
        config = Config()
        db_config = config.get('database')
        
        driver = db_config.get('driver', 'mysql')
        user = db_config.get('user', 'root')
        password = db_config.get('password', 'password')
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 3306)
        database = db_config.get('database', 'iot_platform')
        
        if driver == 'mysql':
            database_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        elif driver == 'sqlite':
            database_url = f"sqlite:///{database}.db"
        else:
            raise ValueError(f"Unsupported database driver: {driver}")
        
        self._engine = create_engine(database_url, echo=db_config.get('echo', True))
        self._SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self._engine)
    
    def get_session(self) -> Session:
        """Get database session"""
        return self._SessionLocal()
    
    def get_engine(self):
        """Get database engine"""
        return self._engine
    
    def close(self):
        """Close database connection"""
        if self._engine:
            self._engine.dispose()


def init_db():
    """Initialize database tables"""
    db = Database()
    Base.metadata.create_all(bind=db.get_engine())


def get_db_session() -> Session:
    """Get database session dependency"""
    db = Database()
    session = db.get_session()
    try:
        yield session
    finally:
        session.close()