import psycopg2
import psycopg2.extensions
from contextlib import closing
from airflow.hooks.postgres_hook import PostgresHook


class RedshiftHook(PostgresHook):
    """
    Interact with Redshift.
    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
    """
    conn_name_attr = 'redshift_conn_id'
    default_conn_name = 'redshift_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        conn = self.get_connection(self.redshift_conn_id)
        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.schema or conn.schema,
            port=conn.port,
            keepalives_idle=60)
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['sslmode', 'sslcert', 'sslkey',
                            'sslrootcert', 'sslcrl', 'application_name',
                            ]:
                conn_args[arg_name] = arg_val

        self.conn = psycopg2.connect(**conn_args)
        return self.conn

    def dictfetchall(self, sql, parameters=None):
        """
        Executes the sql and returns a set of records as dict.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """

        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                self.log.info(sql)
                if parameters is not None:
                    self.log.info(parameters)
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)

                columns = [col[0] for col in cur.description]

                return [
                    dict(zip(columns, row))
                    for row in cur.fetchall()
                ]
