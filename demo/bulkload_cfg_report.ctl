
# [<schema_name>.]table_name
OUTPUT = cfg_report
# database action
TRUNCATE=false
ON_DUPLICATE_KEEP = NEW
#FILTER=bulkload_filter_cellmr_to_5s

# Input data location (absolute path)
INPUT = /home/postgres/data/cfg_report.csv

#INPUT = /home/postgres/data/stat_5m_uemr.csv

LOGFILE=/home/postgres/bulkload_test/logs/cfg_report.log
PARSE_BADFILE=/home/postgres/bulkload_test/logs/cfg_report.bad.log
DUPLICATE_BADFILE=/home/postgres/bulkload_test/logs/cfg_report.duplicate.log

# Input file type
TYPE = CSV
# CSV Fomart Parameters
QUOTE = "\""
ESCAPE = \
DELIMITER = "|"
NULL=""

SKIP=0
LIMIT=INFINITE
PARSE_ERRORS=INFINITE
DUPLICATE_ERRORS=INFINITE

WRITER=PARALLEL
VERBOSE =NO

#CSV_FIELDS=table_name,params_name,description,params_value,default_value,range_operator
#CSV_FIELDS=table_name,params_name,description,params_value,default_value,range_operator,oo,ii,pp
CSV_FIELDS=table_name,params_name,description
#CSV_FIELDS=table_name


#FINAL_FIELDS=table_name,params_name,description,table_name,params_name,pp
#FINAL_FIELDS=table_name,params_name,description,params_value,default_value,range_operator
#FINAL_FIELDS=params_name,,params_value,default_value,range_operator
#FINAL_FIELDS=table_name,params_name,PP,VV,NN,KK
FINAL_FIELDS=table_name,params_name,PP,MM
#FINAL_FIELDS=table_name,"",params_name,,description,params_value,default_value,range_operator
#FINAL_FIELDS= 

