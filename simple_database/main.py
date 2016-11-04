import os
import errno
import json
from datetime import date

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH


def json_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""

    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


class Row(object):
    def __init__(self, *args, **kwargs):
        self.values = args
        self.columns = kwargs.get('columns', None)
        if self.columns:
            for idx, column in enumerate(self.columns):
                setattr(self, column['name'], args[idx])
        else:
            for idx, arg in enumerate(args):
                setattr(self, str(idx), arg)

    def matches_query(self, **kwargs):
        for k, v in kwargs.items():
            if k in [c['name'] for c in self.columns]:
                if getattr(self, k) == v:
                    return True
        return False

    def __repr__(self):
        name_value = dict(zip([c['name'] for c in self.columns], self.values))
        return 'Column: {}'.format(name_value)


class Table(object):
    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name
        self.columns = columns
        self.rows = []

        if not self.load():
            self.save()

    def insert(self, *args):
        if len(self.columns) != len(args):
            raise ValidationError('Invalid amount of field')

        for idx, arg in enumerate(args):
            if not isinstance(arg, eval(self.columns[idx]['type'])):
                raise ValidationError('Invalid type of field "{}":'
                                      ' Given "{}", expected "{}"'
                                      .format(self.columns[idx]['name'],
                                              type(arg).__name__,
                                              self.columns[idx]['type']))
        self.rows.append(Row(*args, columns=self.columns))
        self.save()

    def query(self, **kwargs):
        for row in self.rows:
            # for k, v in kwargs.items():
            if row.matches_query(**kwargs):
                yield row

    def sorted(self, key=None, reverse=False):
        if key and key in self.column_names():
            for row in sorted(self.rows, key=lambda x: getattr(x, key), reverse=reverse):
                yield row
        else:
            raise ValidationError('Invalid sort key')

    def all(self):
        for row in self.rows:
            yield row

    def count(self):
        return len(self.rows)

    def describe(self):
        return self.columns

    def column_names(self):
        return [c['name'] for c in self.columns]

    def get_file_name(self):
        return os.path.join(self.db.db_path, '{}.json'.format(self.name))

    def load(self):
        if os.path.isfile(self.get_file_name()):
            with open(self.get_file_name(), 'r') as in_file:
                json_data = json.load(in_file)
            self.columns = json_data['columns']
            self.rows = [Row(*r, columns=self.columns) for r in json_data['rows']]
            if self.name not in self.db.tables:
                self.db.tables.append(self.name)
                setattr(self.db, self.name, self)
            return True
        return False

    def save(self):
        data = {'columns': self.columns,
                'rows': [r.values for r in self.rows]}
        with open(self.get_file_name(), 'w') as out:
            json.dump(data, out, default=json_serializer)


class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.tables = []
        self.db_path = os.path.join(BASE_DB_FILE_PATH, name)

        if os.path.exists(self.db_path):
            self.load_tables()

    @classmethod
    def create(cls, name):
        try:
            os.makedirs(os.path.join(BASE_DB_FILE_PATH, name))

        except OSError as e:
            if e.errno == errno.EEXIST:
                raise ValidationError('Database with name "{}" already exists.'
                                      .format(name))

    def create_table(self, table_name, columns):
        table = Table(self, table_name, columns=columns)
        self.tables.append(table_name)
        setattr(self, table_name, table)

    def show_tables(self):
        return self.tables

    def load_tables(self):
        for file in os.listdir(self.db_path):
            filename, file_ext = os.path.splitext(file)
            if file_ext == '.json':
                table = Table(self, filename)
                table.load()


def create_database(db_name):
    """
    Creates a new DataBase object and returns the connection object
    to the brand new database.
    """
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    """
    Connectes to an existing database, and returns the connection object.
    """
    return DataBase(name=db_name)
