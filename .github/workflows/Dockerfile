ARG RHEL_VERSION
FROM rockylinux:${RHEL_VERSION}

ARG RHEL_VERSION
ARG PG_VERSION
ARG PG_BULKLOAD_VERSION

ENV PATH /usr/pgsql-${PG_VERSION}/bin:$PATH
ENV PGDATA /var/lib/pgsql/${PG_VERSION}/data


################################################################################
#
# Prerequisite
#
################################################################################

# Install packages for build
RUN dnf update -y
RUN dnf install -y \
        clang gcc git krb5-devel libselinux-devel libzstd-devel lz4-devel make \
        openssl-devel pam-devel readline-devel rpmdevtools zlib-devel

# Install PostgreSQL
RUN if [ "${RHEL_VERSION}" = "8" ]; then \
        dnf install -y --enablerepo=powertools perl-IPC-Run; \
    else \
        dnf install -y --enablerepo=crb perl-IPC-Run; \
    fi
RUN dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-${RHEL_VERSION}-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN dnf -qy module disable postgresql
RUN dnf install -y \
        postgresql${PG_VERSION}-server \
        postgresql${PG_VERSION}-devel \
        postgresql${PG_VERSION}-llvmjit


################################################################################
#
# Build RPMs
#
################################################################################

# Build by postgres user
USER postgres
WORKDIR /var/lib/pgsql/

# Deploy the files required for the build
RUN rpmdev-setuptree
RUN git clone https://github.com/ossc-db/pg_bulkload.git
RUN cp -a pg_bulkload/SPECS/pg_bulkload-pg${PG_VERSION}.spec rpmbuild/SPECS
RUN cd pg_bulkload && \
    git archive VERSION${PG_BULKLOAD_VERSION//./_} \
        --format=tar.gz \
        --prefix=pg_bulkload-${PG_BULKLOAD_VERSION}/ \
        --output=../rpmbuild/SOURCES/pg_bulkload-${PG_BULKLOAD_VERSION}.tar.gz

# Build RPMs
RUN rpmbuild rpmbuild/SPECS/pg_bulkload-pg${PG_VERSION}.spec \
        -bb --define="dist .pg${PG_VERSION}.rhel${RHEL_VERSION}"


################################################################################
#
# Run regression tests
#
################################################################################

USER root
RUN rpm -ivh /var/lib/pgsql/rpmbuild/RPMS/x86_64/*

USER postgres
RUN initdb --no-locale -E UTF8  && \
    pg_ctl -w start && \
    make -C pg_bulkload installcheck
