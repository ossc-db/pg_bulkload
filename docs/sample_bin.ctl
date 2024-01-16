#
# sample_bin.ctl -- Control file to load fixed binary input data
#
#    Copyright (c) 2007-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
OUTPUT = table_name                    # [<schema_name>.]table_name
INPUT = /path/to/input_data_file.data  # Input data location (absolute path)
TYPE = BINARY                          # Input file type

COL = CHAR(10) NULLIF 'nullstring'     # character (10 bytes)
COL = CHAR(40+20)                      # character (20 bytes, offset 40 bytes)
COL = CHAR(20+20)                      # character (20 bytes, offset 20 bytes)
COL = CHAR(60:69)                      # character (between 60 and 69 bytes offset)
COL = VARCHAR(10)                      # character (10 bytes), preserving trailing spaces
COL = INTEGER(2) NULLIF FFFF           # binary smallint column (short in C)
COL = INTEGER(4)                       # binary integer column (int in C)
COL = INTEGER(8)                       # binary bigint column (long in C)
COL = FLOAT(4)                         # binary real column (float in C)
COL = FLOAT(8) NULLIF 0000000000000000 # binary double precision column (double in C)
