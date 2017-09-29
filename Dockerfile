FROM postgres:9.6

COPY . /src
RUN apt-get update \
	&& apt-get install -y \
		libselinux-dev libpam-dev libedit-dev libkrb5-dev libssl-dev \
		build-essential libpq-dev postgresql-server-dev-9.6  
RUN cd /src   \
	&& make   \
	&& make install

FROM postgres:9.6
COPY --from=0 /usr/lib/postgresql/9.6/bin/pg_bulkload /usr/lib/postgresql/9.6/bin/
COPY --from=0 /usr/lib/postgresql/9.6/lib/pg_bulkload.so \
		/usr/lib/postgresql/9.6/lib/pg_timestamp.so /usr/lib/postgresql/9.6/lib/
COPY --from=0 /usr/share/postgresql/9.6/extension/*pg_bulkload* /usr/share/postgresql/9.6/extension/
COPY --from=0 /usr/share/postgresql/9.6/contrib/uninstall_pg_timestamp.sql \
		/usr/share/postgresql/9.6/contrib/pg_timestamp.sql /usr/share/postgresql/9.6/contrib/

ENTRYPOINT [ "/usr/lib/postgresql/9.6/bin/pg_bulkload" ]
