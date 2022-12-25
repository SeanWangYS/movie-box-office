from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch, helpers

class ElasticHook(BaseHook):

    def __init__(self, conn_id='elastic_default', *args, **kwargs):
        super().__init__(*args, **kwargs) 
        
        # get connection info from the conneciton you built in Web UI
        conn = self.get_connection(conn_id) 
        conn_config = {}
        hosts = []
        if conn.host:
            hosts = conn.host.split(',')
        if conn.port:
            conn_config['port'] = int(conn.port)
        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        self.es = Elasticsearch(hosts, **conn_config)

    def info(self):
        return self.es.info()

    def index_exists(self, index):
        return self.es.indices.exists(index=index)

    def create_index(self, index, body):
        return self.es.indices.create(index=index, body=body)

    def add_doc(self, index, doc_type, doc):
        return self.es.index(index=index, doc_type=doc_type, doc=doc)

    def add_docs(self, actions):
        return helpers.bulk(self.es, actions)
        
# for adding the elastic hook to the plugin system manager
class AirflowElasticPlugin(AirflowPlugin):
    name = 'elasticsearch' # the name wiil be registed on Web UI interface
    hooks = [ElasticHook]