import os
import yaml

from airflow import DAG
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime
from operators.mysql_s3_operator import MySqlS3Operator
from operators.uuid_generator import UUIDGenerator
from plugins.redshift_plugin.constants import RedshiftLoadType
from plugins.redshift_plugin.operators.s3_redshift_operator import S3RedshiftOperator
from plugins.redshift_plugin.operators.redshift_schema_operator import RedshiftSchemaOperator
from plugins.redshift_plugin.operators.redshift_operator import RedshiftOperator
from plugins.redshift_plugin.operators.redshift_migration_operator import RedshiftMigrationOperator
from utils.constants import DEFAULT_MYSQL_BATCH_SIZE, DEFAULT_MIGRATION_VERSION, REDSHIFT_COPY_OPTIONS, XcomParam
from utils.slack import task_fail_slack_alert, task_retry_slack_alert


log = LoggingMixin().log


default_args = {
    'owner': 'devop',
    'depends_on_past': False,
    'start_date': datetime(2018, 12, 11),
    'on_failure_callback': task_fail_slack_alert,
    'on_retry_callback': task_retry_slack_alert,
    'task_concurrency': 1,
}


def create_mysql_s3_redshift_migration_dag(
    default_args,
    dag_id,
    migration_config,
):

    dag = DAG(dag_id, catchup=False, default_args=default_args, schedule_interval=None)

    s3_bucket = 'buzzvil-archive'
    s3_prefix = '/'.join(['migrate_mysql_redshift', migration_config['this_version']])

    with dag:

        validate_migration_dependency = RedshiftMigrationOperator(
            service_name=migration_config['service_name'],
            migration_version=migration_config['this_version'],
            confirm_migration=False,
            task_id='validate_migration_dependency',
        )

        generate_uuid = UUIDGenerator(
            task_id='generate_uuid',
        )

        create_staging_table = RedshiftSchemaOperator(
            table_name=migration_config['redshift']['table_name'] + '_new',
            create_table=True,
            create_table_syntax=migration_config['redshift']['create_table_syntax'],
            drop_existing_table=True,

            task_id='create_staging_table',
        )

        stage_mysql_data = MySqlS3Operator(
            mysql_conn_id=migration_config['mysql']['conn_id'],
            mysql_table_name=migration_config['mysql']['table_name'],
            mysql_columns=migration_config['mysql']['fields'],
            increment_key=migration_config['mysql']['increment_key'],
            increment_key_type=migration_config['mysql']['increment_key_type'],
            s3_conn_id='adfit_s3',
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            s3_prefix_append_dag_run_uuid=True, # Required to prevent dirty writes to s3 intermediate location caused by parallel executions
            batch_size=DEFAULT_MYSQL_BATCH_SIZE,

            task_id='unload_mysql_data_to_s3',
        )

        copy_s3_data = S3RedshiftOperator(
            table_name=migration_config['redshift']['table_name'] + '_new',
            columns=migration_config['redshift']['fields'],
            increment_key=migration_config['redshift']['increment_key'],
            unique_key_list=migration_config['redshift']['unique_key_list'],
            copy_method=RedshiftLoadType(migration_config['redshift']['copy_method']),

            redshift_conn_id='redshift',

            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,

            copy_option_list=REDSHIFT_COPY_OPTIONS['default_csv'],

            task_id='copy_s3_data',
        )

        swap_redshift_tables = RedshiftOperator(
            sql="""
                {% raw %}
                BEGIN;
                ALTER TABLE {param[table_name]} RENAME TO {param[table_name]}_old_{{param[dag_run_uuid]}};
                ALTER TABLE {param[table_name]}_new RENAME TO {param[table_name]};
                END;
                {% endraw %}
                """,
            redshift_conn_id='redshift',
            param_dict=dict({'table_name': migration_config['redshift']['table_name']}),
            xcom_params=[
                XcomParam(xcom_source_task_id='generate_uuid', xcom_param_key='dag_run_uuid'),
            ],
            task_id='swap_redshift_tables',
        )

        update_migration_version = RedshiftMigrationOperator(
            service_name=migration_config['service_name'],
            migration_version=migration_config['this_version'],
            confirm_migration=True,
            task_id='update_migration_version',
        )

        validate_migration_dependency >> generate_uuid >> create_staging_table >> stage_mysql_data >> copy_s3_data >> swap_redshift_tables >> update_migration_version

    return dag


migration_config_files = []

for f in os.scandir('migrations'):
    if f.is_dir():
        service = f.name
        folder = f.path

        last_migration_version = Variable.setdefault(key='_'.join([service, 'migration_version']), default=DEFAULT_MIGRATION_VERSION)
        last_migration_version_number = int(last_migration_version.split('__')[0])
        last_migration_version_time = last_migration_version.split('__')[2]

        for f in os.scandir(folder):
            # Note : the last migration should be visible in UI for possible checks
            log.info('Checking migration version for {0}, for migration_version {1} and migration time {2}'.format(os.path.splitext(f.name)[0], last_migration_version_number, last_migration_version_time))
            if f.is_file() and int(os.path.splitext(f.name)[0].split('__')[0]) >= last_migration_version_number and os.path.splitext(f.name)[0].split('__')[2] >= last_migration_version_time:
                migration_config_files.append(f.path)

for config_file in migration_config_files:
    with open(config_file, 'r') as migration_config:
        migration_config = yaml.load(migration_config)
        log.info('Current config {0}'.format(migration_config['this_version']))

    if migration_config and migration_config['migration_type'] == 'mysql_redshift':
        log.info('Creating config {0}'.format(migration_config['this_version']))
        dag_id = '_'.join(['mysql_redshift_migration', migration_config['this_version'], ])

        globals()[dag_id] = create_mysql_s3_redshift_migration_dag(
            default_args=default_args,
            migration_config=migration_config,

            dag_id=dag_id,
        )
