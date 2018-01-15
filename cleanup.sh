PMOD="json-file-consumer"
VENV="venv"
PROJECT_BASE="/research_data/code/infosec-concierge/"
PROJECT=$PROJECT_BASE/$PMOD/

VIRTUAL_ENV=$PROJECT/$VENV
PYTHON="/usr/bin/python3" #$VIRTUAL_ENV"/bin/python3"
SITE_PACKAGES=$HOME/.local/lib/python3.5/site-packages/

# cleanup local directory
rm -rf $VIRTUAL_ENV/dist-packages/* \
      $PROJECT/src/$PMOD.egg-info/ \
      $PROJECT/dist/ \
      $PROJECT/build/
      $SITE_PACKAGES/json_file_consumer* \
      $SITE_PACKAGES/task_blox* \

rm -r $VIRTUAL_ENV


