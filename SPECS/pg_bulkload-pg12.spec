# SPEC file for pg_bulkload on PostgreSQL 11
# Copyright (C) 2009-2021 NIPPON TELEGRAPH AND TELEPHONE CORPORATION

%define sname                   pg_bulkload
%define pgmajorversion  12

%define _prefix                 /usr/pgsql-%{pgmajorversion}
%define _libdir                 %{_prefix}/lib
%define _bcdir			%{_libdir}/bitcode/pg_bulkload

Summary:        High speed data load utility for PostgreSQL
Name:           %{sname}
Version:        3.1.17
Release:        1%{?dist}
License:        BSD
Group:          Applications/Databases
# You can get the tarball by following: https://github.com/ossc-db/pg_bulkload/archive/%{version}.tar.gz
Source0:        %{sname}-%{version}.tar.gz
URL:            http://ossc-db.github.io/pg_bulkload/index.html
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)

BuildRequires:  postgresql12-devel, postgresql12
Requires:       postgresql12


%description
pg_bulkload provides high-speed data loading capability to PostgreSQL users.

When we load huge amount of data to a database, it is common situation that data set to be loaded is valid and consistent. For example, dedicated tools are used to prepare such data, providing data validation in advance. In such cases, we'd like to bypass any overheads within database system to load data as quickly as possible. pg_bulkload is developed to help such situations. Therefore, it is not pg_bulkload's goal to provide detailed data validation. Rather, pg_bulkload asumes that loaded data set is validated by separate means. If you're not in such situation, you should use COPY command in PostgreSQL.


%package llvmjit
Requires: postgresql12-server, postgresql12-llvmjit
Requires: pg_bulkload = 3.1.17
Summary:  Just-in-time compilation support for pg_bulkload

%description llvmjit
Just-in-time compilation support for pg_bulkdload

%prep
rm -rf %{_libdir}/pgxs/src/backend/

%setup -n %{sname}-%{version}

%build
USE_PGXS=1 make %{?_smp_mflags} MAJORVERSION=%{pgmajorversion}

%install
%define pg_contribdir %{_datadir}/contrib
%define pg_extensiondir %{_datadir}/extension

rm -rf %{buildroot}

install -d %{buildroot}%{_bindir}
install -d %{buildroot}%{_libdir}
install -d %{buildroot}%{pg_contribdir}
install -d %{buildroot}%{pg_extensiondir}
install -d %{buildroot}%{_bcdir}

install -m 755 bin/pg_bulkload                 %{buildroot}%{_bindir}/pg_bulkload
install -m 755 bin/postgresql                  %{buildroot}%{_bindir}/postgresql
install -m 755 lib/pg_bulkload.so              %{buildroot}%{_libdir}/pg_bulkload.so
install -m 644 lib/pg_bulkload.bc %{buildroot}%{_bcdir}/pg_bulkload.bc

install -m 644 lib/pg_bulkload.sql             %{buildroot}%{pg_contribdir}/pg_bulkload.sql
install -m 644 lib/uninstall_pg_bulkload.sql   %{buildroot}%{pg_contribdir}/uninstall_pg_bulkload.sql
install -m 644 lib/pg_bulkload.control         %{buildroot}%{pg_extensiondir}/pg_bulkload.control
install -m 644 lib/pg_bulkload--1.0.sql        %{buildroot}%{pg_extensiondir}/pg_bulkload--1.0.sql
install -m 644 lib/pg_bulkload--unpackaged--1.0.sql         %{buildroot}%{pg_extensiondir}/pg_bulkload--unpackaged--1.0.sql

# sample_*.ctl files are needed for rpm users.
# %{sname}-%{version} is the same path with "%setup -n"'s argument.
install -m 644 docs/sample_bin.ctl              %{buildroot}%{pg_contribdir}/sample_bin.ctl
install -m 644 docs/sample_csv.ctl              %{buildroot}%{pg_contribdir}/sample_csv.ctl

%files
%defattr(755,root,root,755)
%{_bindir}/pg_bulkload
%{_bindir}/postgresql
%{_libdir}/pg_bulkload.so
%defattr(644,root,root,755)
#%doc README.pg_bulkload
%{pg_contribdir}/pg_bulkload.sql
%{pg_contribdir}/uninstall_pg_bulkload.sql
%{pg_contribdir}/sample_bin.ctl
%{pg_contribdir}/sample_csv.ctl
%{pg_extensiondir}/pg_bulkload.control
%{pg_extensiondir}/pg_bulkload--1.0.sql
%{pg_extensiondir}/pg_bulkload--unpackaged--1.0.sql

%files llvmjit
%defattr(0755,root,root)
%{_bcdir}
%defattr(0644,root,root)
%{_bcdir}/pg_bulkload.bc

%clean
rm -rf %{buildroot}
rm -rf %{_libdir}/pgxs/src/backend/

%changelog
* Fri Feb 05 2021 - Moon Insung <insung.moon.gk@hco.ntt.co.jp> 3.1.17-1
* Wed Jan 22 2020 - Moon Insung <insung.moon.gk@hco.ntt.co.jp> 3.1.16-1
- Update to pg_bulkload 3.1.16
- Warn users of some risks of using parallel/multi-process mode
- Document restriction that pg_bulkload supports only tables of "heap" access method
