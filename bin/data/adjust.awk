BEGIN \
{\
	FS="/" \
}\
{\
	if (NF > 2) \
	{\
		gsub(" [a-zA-z]:.*$"," ",$1);\
		print $1 ".../" $NF\
	}\
	else\
	{\
		gsub(" on .*$", " on <TIMESTAMP>");\
		gsub("CPU [0-9]+\\.[0-9][0-9]", "CPU <TIME>");\
		gsub("/[0-9]+\\.[0-9][0-9]", "/<TIME>");\
		gsub("elapsed [0-9]+\\.[0-9][0-9]", "elapsed <TIME>");\
		gsub("pg_bulkload [0-9]+\\.[0-9]+\\.[0-9]+", "pg_bulkload <VERSION>");\
		print\
	}\
}\
