import os
import yaml

from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

from utils.constants import DEFAULT_MIGRATION_VERSION


class RedshiftMigrationOperator(BaseOperator):
    """
    Manages db migration version by checking for dependencies

    :param service_name: Service name to which the data belongs
    :type service_name: string

    :param migration_version: The version of this migration.
    :type migration_version: string

    :param confirm_migration: Whether to confirm migration after validating migration dependency
    :type confirm_migration: boolean
    """

    @apply_defaults
    def __init__(
        self,
        service_name,
        migration_version,
        confirm_migration,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.service_name = service_name
        self.migration_version = migration_version
        self.migration_variable_key = '_'.join([service_name, 'migration_version', ])
        self.confirm_migration = confirm_migration
        self.next_migration_version = None

    def execute(self, context):
        self._validate_dependency()

    def _validate_dependency(self, ):
        migration_config_files = []
        migration_dir = '/'.join(['migrations', self.service_name])

        latest_migration_version = Variable.get(key=self.migration_variable_key)

        for f in sorted(os.scandir(migration_dir), key=lambda x: (x.is_dir(), int(x.name.split('__')[0]))):
            if f.is_file():
                migration_config_files.append((f.path, f.name))

        previous_version = DEFAULT_MIGRATION_VERSION
        for config_file, file_name in migration_config_files:
            self.log.info("Inspecting migration file {0} for dependency".format(file_name))

            with open(config_file, 'r') as migration_config:
                migration_config = yaml.load(migration_config)
                if migration_config['previous_version'] == previous_version:
                    self.log.info("Migration dependency for {0} matches".format(os.path.splitext(file_name)[0]))
                    if migration_config['previous_version'] == latest_migration_version:
                        self.log.info("{0} is the version next in line for migration".format(migration_config['this_version']))
                        self.next_migration_version = migration_config['this_version']
                    pass
                else:
                    raise Exception("Migration dependency is corrupt. current file: {0} does not match the previous_version: {1}".format(
                        os.path.splitext(file_name)[0],
                        previous_version
                    ))
                previous_version = migration_config['this_version']
        else:
            self.log.info("Checking if the current DAG's migration version is actually next in line for the migration")
            if self.migration_version == self.next_migration_version:
                self.log.info("Current DAG's migration : {0} is next in line!".format(self.next_migration_version))
                if self.confirm_migration is True:
                    self._confirm_migration()
            else:
                raise Exception("Current DAG's migration is not next in line! Please make sure you are running the right migration version")

    def _confirm_migration(self, ):
        self.log.info('Updating last migration version from {0} to {1}'.format(Variable.get(key=self.migration_variable_key), self.migration_version))
        Variable.set(key=self.migration_variable_key, value=self.migration_version)
