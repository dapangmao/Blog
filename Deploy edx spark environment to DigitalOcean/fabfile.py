from fabric.api import cd, env, puts, sudo, run
from fabric.contrib.files import exists

env.hosts = ['104.236.12.85']
env.user = 'root'
env.password = '12345'

PROJECT = 'ipython-spark'
CURRENT_VERSION = 'spark-1.4.1-bin-hadoop2.6'
CURRENT_URL = 'http://d3kbcqa49mib13.cloudfront.net/' + CURRENT_VERSION + '.tgz'
SPARK_PATH = "/root/{0}".format(CURRENT_VERSION)

SUPERVISOR_CONF = """
[program: {0}]
command=ipython notebook --profile=nbserver 
autostart=true
autorestart=true
user=root
log_stderr=true
logfile=/var/log/ipython.log
""".format(PROJECT)

NOTEBOOK_CONF = """
# Kernel config
c = get_config()
# Notebook config
c.NotebookApp.certfile = u"/root/.ipython/mycert.pem"
c.NotebookApp.keyfile = u"/root/.ipython/mycert.pem"
c.NotebookApp.ip = "*"
c.NotebookApp.open_browser = False
c.NotebookApp.password = u"{0}"
# Set up the port
c.NotebookApp.port = 9999
c.NotebookApp.notebook_dir = u"/root"
"""

SPARK_STARTUP = """import os, sys
SPARK_HOME = os.environ["SPARK_HOME"] = "{0}"
# Add the PySpark/py4j to the Python Path
sys.path.insert(0, os.path.join(SPARK_HOME, "python/lib/py4j-0.8.2.1-src.zip"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
execfile(os.path.join(SPARK_HOME, "python/pyspark/shell.py"))
""" .format(SPARK_PATH)

def install_basics():
    sudo('apt-get -q update')
    sudo('apt-get install -qy python-pip')
    sudo('apt-get install -qy git')
    sudo('apt-get install -qy supervisor unzip')
    sudo('apt-get install -qy python-numpy python-scipy python-matplotlib')
    sudo('apt-get install -qy default-jre')
    sudo('pip install "ipython[all]"')
    
def make_ssl():
    with cd('/root/.ipython'):
        sudo('openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mycert.pem -out mycert.pem -subj "/CN=edx/O=spark/C=US"')

def download_spark():
    current_file = CURRENT_VERSION + '.tgz'
    if not exists('/root/{}'.format(CURRENT_VERSION)):
        run('wget {}'.format(CURRENT_URL))
        run('tar xf {}'.format(current_file))
        run('rm {}'.format(current_file))

def download_edx_data():
    if not exists('/root/edx'):
        run('git clone https://github.com/spark-mooc/mooc-setup.git edx')
        with cd('/root/edx'):
            run('git clone https://github.com/spark-mooc/test_helper.git')
            run('mv test_helper/test_helper/* test_helper/')
            run('mkdir data')
    with cd('/root/edx/data'):
        if not exists('cs100.zip'):
            run('wget https://github.com/spark-mooc/cs100-data/raw/master/cs100.zip')
        if not exists('cs190.zip'):
            run('wget https://github.com/spark-mooc/cs190-data/raw/master/cs190.zip')
            run('unzip cs100.zip')
            run('unzip cs190.zip')
            run('rm *.zip')
            
def get_pwd_hash(pwd):
    script = "from IPython.lib import passwd; print passwd(str({0}))".format(pwd)
    with cd('/root/'):
        run("echo '{}' > pwd.py".format(script))
        output = run("python pwd.py")   
    notebook_conf = NOTEBOOK_CONF.format(str(output).strip())
    run('ipython profile create nbserver')
    with cd('/root/.ipython/profile_nbserver'):
        run("echo '{}' >> ipython_notebook_config.py".format(notebook_conf))
        run("echo '{}' >> ipython_config.py".format('c.IPKernelApp.matplotlib = "inline"'))
    with cd('/root/.ipython/profile_nbserver/startup'):
        current = '00-' + PROJECT + '.py'
        if not exists(current):
            run("echo '{0}' > {1}".format(SPARK_STARTUP, current))

def adjust_supervisor():
    if not exists('/etc/supervisor/conf.d/{}.conf'.format(PROJECT)):
        sudo("echo '{}' ".format(SUPERVISOR_CONF) + \
            '> /etc/supervisor/conf.d/{}.conf'.format(PROJECT))
    sudo('supervisorctl reread')
    sudo('supervisorctl update')
    
def run_ipython():
    sudo('supervisorctl start {}'.format(PROJECT))
    puts('Now go to https://{}:9999 to view the app'.format(env.hosts[0]))

def deploy_edx_spark():
    install_basics()
    make_ssl()
    download_spark()
    download_edx_data()
    get_pwd_hash(12345)
    adjust_supervisor()
    run_ipython()
