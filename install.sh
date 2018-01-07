PMOD="json-file-consumer"
VENV="venv"
PROJECT_BASE="/research_data/code/git/"
PROJECT=$PROJECT_BASE/$PMOD/

VIRTUAL_ENV=$PROJECT/$VENV
PYTHON=$VIRTUAL_ENV"/bin/python"

bash cleanup.sh

virtualenv -p /usr/bin/python3 $VIRTUAL_ENV
$PYTHON $PROJECT_BASE/$SPOJ/setup.py install
$PYTHON setup.py install

