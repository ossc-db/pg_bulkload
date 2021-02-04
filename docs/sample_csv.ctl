#
# sample_csv.ctl -- Control file to load CSV input data
#
#    Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
OUTPUT = foo                   # [<schema_name>.]table_name
INPUT = /tmp/foo.csv  # Input data location (absolute path)
TYPE = CSV                            # Input file type
QUOTE = "\""                          # Quoting character
ESCAPE = \                            # Escape character for Quoting
DELIMITER = ","                       # Delimiter
