import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import  Optional
class DBConnection:
    def __init__(self, db_type, host, user, pw, name, port=None):
        self.db_type = db_type.lower()
        self.host = host
        self.user = user
        self.pw = pw
        self.name = name
        self.port = port or (1433 if self.db_type == 'mssql' else 5432)
        self.logger = logging.getLogger("services.database")
        self.engine = None
        self.connection = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._max_workers = 1

    def connect(self):
        try:
            if self.db_type == 'mssql':
                connection_string = f"mssql+pymssql://{self.user}:{self.pw}@{self.host}:{self.port}/{self.name}"
            elif self.db_type == 'postgresql':
                connection_string = f"postgresql://{self.user}:{self.pw}@{self.host}:{self.port}/{self.name}"
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

            self.engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=5,
                pool_pre_ping=True,
                pool_recycle=3600  # seconds
            )
            self.connection = self.engine.connect()
            self.logger.info("Connected to database")
        except SQLAlchemyError as e:
            self.logger.exception("DB connection error")
            raise e

    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()
            self.logger.info("Disconnected from database")
        except SQLAlchemyError as e:
            self.logger.exception("Error during disconnect")
            raise e

    def execute_query_old(self, query, params=None):
        if not self.connection:
            raise ConnectionError("Database not connected.")
        try:
            result = self.connection.execute(text(query), params or {})
            return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            self.logger.exception("Query failed: %s", query)
            raise e

    def execute_query(self, query, params=None):
        if not self.connection:
            raise ConnectionError("Database not connected.")
        try:
            result = self.connection.execute(text(query), params or {})
            return [dict(row) for row in result.mappings()]
        except SQLAlchemyError as e:
            self.logger.exception("Query failed: %s", query)
            try:
                self.connection.rollback()  # <- Add this line!
            except Exception as rb_exc:
                self.logger.error("Rollback failed: %s", rb_exc)
            raise e
    async def aexecute_query(self, query, params=None):
        """
                Runs execute_query in a dedicated thread so the event loop isn't blocked.
                Default single-thread to keep SQLAlchemy Connection thread-safe.
                """
        if self._executor is None:
            # Keep it lazy; 1 worker == same thread always.
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers, thread_name_prefix="db-conn")
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: self.execute_query(query, params))