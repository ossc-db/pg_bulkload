#
# sample_csv.ctl -- Control file to load CSV input data
#
#    Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#
TABLE = table_name                       # [<schema_name>.]table_name
INFILE = /path/to/input_data_file.data   # Input data location(absolute path)
TYPE = CSV                               # Input file type
QUOTE = "\""                             # Quoting character
ESCAPE = \                               # Escape character for Quoting
DELIMITER = ","                          # Delimiter
