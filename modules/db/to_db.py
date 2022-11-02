import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


DB_SQLITE = './db/db_collect.db'


# create table from df
def to_sql_k(self, frame, name, if_exists='append', 
             index=False, index_label=None, schema=None, 
             chunksize=None, dtype=None, **kwargs):
    if dtype is not None:
        from sqlalchemy.types import to_instance, TypeEngine
        for col, my_type in dtype.items():
            if not isinstance(to_instance(my_type), TypeEngine):
                raise ValueError('The type of %s is not a SQLAlchemy '
                                 'type ' % col)

    table = pd.io.sql.SQLTable(name, self, frame=frame, index=index,
                               if_exists=if_exists, index_label=index_label,
                               schema=schema, dtype=dtype, **kwargs)
    table.create()
    table.insert(chunksize)

# create table sqlite
def to_sqlite(df, name_table):
    engine = create_engine(f'sqlite:///{DB_SQLITE}')
    pandas_sql = pd.io.sql.pandasSQL_builder(engine, schema=None)

    to_sql_k(pandas_sql, frame=df, name=name_table)
