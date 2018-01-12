BASE_CUR=$PWD
EVE_FILE=eve-files.zip
EVE_DATA_ZIP=$BASE_CUR/$EVE_FILE
BASE_TEST=$BASE_CUR/"testing__"
SIMULATED="/var/log/suricata/"
UNZIP_DIR=$BASE_TEST/$SIMULATED

# create directory
mkdir -p $UNZIP_DIR
cd $UNZIP_DIR
echo $BASE_CUR
echo $EVE_DATA_ZIP
unzip $EVE_DATA_ZIP
cd $BASE_CUR
