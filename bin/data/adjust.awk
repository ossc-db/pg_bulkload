BEGIN {FS="/"}{if(NF>2){sub(" [a-zA-z]:.*$"," ",$1);print $1 ".../" $NF}else{sub(" on .*$", " on <TIMESTAMP>");gsub("[0-9]+\\.[0-9][0-9]", "<TIME>");print}}
