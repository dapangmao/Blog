from __future__ import with_statement
from fabric.api import cd, env, sudo, run, settings, local, abort
from fabric.contrib.files import exists

IP = 'unknown'
if env.hosts:
    IP = env.hosts[0]

env.user = 'root'

CURRENT = 'spark-1.3.0-bin-hadoop2.4'
CURRENT_URL = 'http://d3kbcqa49mib13.cloudfront.net/spark-1.3.0-bin-hadoop2.4.tgz'
SPARK_MASTER = 'mesos://zk://{}:2181/mesos'.format(IP)
SPARK_EXECUTOR_URI = 'hdfs://{0}/spark/{1}.tgz'.format(IP, CURRENT)

SPARK_ENV_PARAMETER = \
"""
export MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so
export SPARK_EXECUTOR_URI={0}
export MASTER={1}
export SPARK_LOCAL_IP={2}
export SPARK_PUBLIC_DNS={3}
""".format(SPARK_EXECUTOR_URI, SPARK_MASTER, IP, IP)

MASTER_ENV_PARAMETER = \
"""
export PATH={0}/bin:$PATH
export SPARK_MASTER={1}
""".format(CURRENT, SPARK_MASTER)

def set_openvpn(ovpn_file):
    with settings(warn_only = True):
        reply = local('locate openvpn')
    if reply.failed:
        local('sudo apt-get install openvpn bridge-utils')
    local('sudo openvpn --config {}'.format(ovpn_file))
    
def dismiss_openvpn():
    local('sudo killall openvpn')

def install_spark():
    if IP == 'unknown':
        abort('Master IP has to be specified')
    current_file = CURRENT + '.tgz'
    if not exists('/root/{}'.format(CURRENT)):
        run('wget {}'.format(CURRENT_URL))
        run('tar xf {}'.format(current_file))
    with settings(warn_only = True):
        response = run('hdoop fs -ls /spark')
    if response.failed:
        run('hadoop fs -mkdir /spark')
    if exists('/root/{}'.format(CURRENT)):
        run('hadoop fs -put {} /spark'.format(current_file))

def configure_spark():
    with cd('/root/{}/conf'.format(CURRENT)):
        sudo('cp spark-env.sh.template spark-env.sh')
        sudo('cat >> spark-env.sh <<EOF {}'.format(SPARK_ENV_PARAMETER))
        sudo('cp spark-defaults.conf.template spark-defaults.conf')
        sudo('echo spark.executor.uri {} >> spark-defaults.conf'\
            .format(SPARK_EXECUTOR_URI))
        run('sed "s/log4j.rootCategory=INFO/log4j.rootCategory=WARN/" \
            < log4j.properties.template > log4j.properties')

def install_ipython():
    sudo('apt-get install -y vim')
    sudo('pip install ipython')
    with cd('/root'):
        run('cat >> .profile <<EOF {}'.format(MASTER_ENV_PARAMETER))
        run('echo alias spark-ipython=\"IPYTHON=1 ~/{}/bin/pyspark\" >> .bashrc'\
            .format(CURRENT))
            
def deploy_spark():
    install_spark()
    configure_spark()
    install_ipython()
    