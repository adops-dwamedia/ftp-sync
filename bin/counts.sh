#!/usr/bin/env bash

SQL_PATH="../SQL/"
DATA_PATH="../downloaded/"
LOG_PATH="../logs/"
TMP_PATH="../tmp/"

USER="tomb"
HOST="localhost"
DB="DWA_SF_Cookie"
PW=`cat ../../pw/mysql`

mysql -u$USER -p$PW $DB -e "SELECT DATE(EventDate) d, COUNT(*) FROM MM_Standard_Copy GROUP BY d UNION ALL SELECT DATE(EventDate) d, COUNT(*) FROM MM_Standard GROUP BY d;" > $TMP_PATH/standard_count.txt

mysql -u$USER -p$PW $DB -e "SELECT DATE(InteractionDate) d, COUNT(*) FROM MM_Rich_Copy GROUP BY d UNION ALL SELECT DATE(InteractionDate) d, COUNT(*) FROM MM_Rich GROUP BY d;" > $TMP_PATH/rich_count.txt

mysql -u$USER -p$PW $DB -e "SELECT DATE(ConversionDate) d, COUNT(*) FROM MM_Conversion_Copy GROUP BY d UNION ALL SELECT DATE(ConversionDate) d, COUNT(*) FROM MM_Conversion GROUP BY d;" > $TMP_PATH/conversion_count.txt
