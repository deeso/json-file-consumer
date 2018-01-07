PMOD="json-file-consumer"
VENV="venv"
PROJECT_BASE="/research_data/code/git/"
PROJECT=$PROJECT_BASE/$PMOD/

VIRTUAL_ENV=$PROJECT/$VENV
PYTHON=$VIRTUAL_ENV"/bin/python"

# cleanup local directory
rm -r $VIRTUAL_ENV/dist-packages/* \
      $PROJECT/src/$PMOD.egg-info/ \
      $PROJECT/dist/ \
      $PROJECT/build/

rm -r $VIRTUAL_ENV


