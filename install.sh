PMOD="json-file-consumer"
VENV="venv"
PROJECT_BASE="/research_data/code/infosec-concierge/"
PROJECT=$PROJECT_BASE/$PMOD/

VIRTUAL_ENV=$PROJECT/$VENV
PYTHON=/usr/bin/python3 #$VIRTUAL_ENV"/bin/python3"
PIP=/usr/bin/pip3 #$VIRTUAL_ENV"/bin/pip3"

bash cleanup.sh

virtualenv -p /usr/bin/python3 $VIRTUAL_ENV
$PIP install -r $PROJECT/requirements.txt .


