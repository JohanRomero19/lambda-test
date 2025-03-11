from psycopg2 import OperationalError, ProgrammingError, DataError, IntegrityError
from psycopg2.extras import execute_batch
from psycopg2 import connect
from dotenv import load_dotenv

import json
import os

from commons.logger import LOGGER

class Database:
    def __init__(self):
        load_dotenv()
        self.dbname = os.getenv('DBNAME')
        self.user = os.getenv('USER')
        self.password = os.getenv('PASSWORD')
        self.host = os.getenv('HOST')
        self.port = os.getenv('PORT')
        self.columns = json.loads(os.getenv('COLUMNS', '{}'))
        LOGGER.info('Environment variables loaded!')
    
    def connect(self):
        """Establishes a connection to the database."""
        return connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def insert_data(self, data, origin: str):
        """
        Inserts data into the database.
        - param data: list of dictionaries containing the data
        - param origin: table name
        return: dictionary response
        """
        try:
            if origin not in self.columns:
                raise KeyError(f"Invalid origin: {origin}")

            column = self.columns[origin]
            values = '(%s, %s)' if origin in ['jobs', 'departments'] else '(%s, %s, %s, %s, %s)'
            query = f'INSERT INTO {origin} {column} VALUES {values}'

            conn = self.connect()
            LOGGER.info('Database connection established!')
            
            data_tuples = self.prepare_data(data, origin)
            if not data_tuples:
                raise ValueError("Data not generated for insertion!")
            
            LOGGER.info('Data prepared!')
            
            cursor = conn.cursor()
            execute_batch(cursor, query, data_tuples)
            conn.commit()
            LOGGER.info('Data inserted!')

            return self.set_response("Successful data insertion", 200)
        
        except (KeyError, OperationalError, IntegrityError, ProgrammingError, DataError) as e:
            LOGGER.error(f"Database error: {e}")
            return self.set_response("Internal server error", 500)
        except Exception as e:
            LOGGER.error(f"Unexpected error: {e}")
            return self.set_response("Internal server error", 500)
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    def prepare_data(self, data, origin: str):
        """Prepares data for insertion."""
        if origin == 'jobs':
            return [(row['id'], row['job']) for row in data]
        elif origin == 'departments':
            return [(row['id'], row['department']) for row in data]
        elif origin == 'employees':
            processed_data = []
            for row in data:
                try:
                    job_id = row.get('job_id') if row.get('job_id') not in ["", None] else None
                    department_id = row.get('department_id') if row.get('department_id') not in ["", None] else None
                    
                    if row['id'] and row['name'] and row['datetime']:
                        processed_data.append((
                            row['id'],
                            row['name'],
                            row['datetime'],
                            department_id,
                            job_id
                        ))
                except KeyError as e:
                    LOGGER.warning(f"Registro con datos incompletos descartado: {row}, Error: {e}")
            
            return processed_data if processed_data else None
            
        return None
    
    @staticmethod
    def set_response(message: str, status: int):
        """Creates a response dictionary."""
        return {"message": message or "Internal server error", "status": status or 500}