#!/usr/bin/env bash

SQL_PATH="../SQL/"
DATA_PATH="../downloaded/"
LOG_PATH="../logs/"
TMP_PATH="../var/"

USER="tomb"
HOST="localhost"
DB="DWA_SF_Cookie"
PW=`cat ../../pw/mysql`

mysql -u$USER -p$PW $DB -e "SELECT DATE(EventDate) d, COUNT(*) FROM MM_Standard GROUP BY d;" > $TMP_PATH/standard_count.txt
echo "Standard counts calculated"
mysql -u$USER -p$PW $DB -e "SELECT DATE(InteractionDate) d, COUNT(*) FROM MM_Rich GROUP BY d;" > $TMP_PATH/rich_count.txt
echo "Rich counts calculated"
mysql -u$USER -p$PW $DB -e "SELECT DATE(ConversionDate) d, COUNT(*) FROM MM_Conversion GROUP BY d;" > $TMP_PATH/conversion_count.txt
echo "Conversion counts calculated"
echo "Counts complete"
