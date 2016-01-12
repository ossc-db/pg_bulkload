#
# sample_csv.ctl -- Control file to load CSV input data
#
#    Copyright (c) 2007-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
OUTPUT = table_name                   # [<schema_name>.]table_name
INPUT = /path/to/input_data_file.csv  # Input data location (absolute path)
TYPE = CSV                            # Input file type
QUOTE = "\""                          # Quoting character
ESCAPE = \                            # Escape character for Quoting
DELIMITER = ","                       # Delimiter
