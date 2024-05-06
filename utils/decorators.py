def get_dag_run_uuid(func):
    from functools import wraps

    @wraps(func)
    def wrapper(self, context):
        task_instance = context['task_instance']
        self.dag_run_uuid = task_instance.xcom_pull(task_ids='generate_uuid', key='dag_run_uuid')['dag_run_uuid']
        self.log.info("Pulling uuid : {0} from xcom".format(self.dag_run_uuid))
        return func(self, context)
    return wrapper
