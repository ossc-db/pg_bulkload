# SPEC file for pg_bulkload on PostgreSQL 9.2
# Copyright (C) 2009-2015 NIPPON TELEGRAPH AND TELEPHONE CORPORATION

%define sname                   pg_bulkload
%define pgmajorversion  9.2

%define _prefix                 /usr/pgsql-%{pgmajorversion}
%define _libdir                 %{_prefix}/lib

Summary:        High speed data load utility for PostgreSQL
Name:           %{sname}
Version:        3_1_9
Release:        1%{?dist}
License:        BSD
Group:          Applications/Databases
Source0:        https://github.com/ossc-db/pg_bulkload/archive/VERSION%{version}.tar.gz
URL:            http://ossc-db.github.io/pg_bulkload/index.html
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)

BuildRequires:  postgresql92-devel, postgresql92
Requires:       postgresql92


%description
pg_bulkload provides high-speed data loading capability to PostgreSQL users.

When we load huge amount of data to a database, it is common situation that data set to be loaded is valid and consistent. For example, dedicated tools are used to prepare such data, providing data validation in advance. In such cases, we'd like to bypass any overheads within database system to load data as quickly as possible. pg_bulkload is developed to help such situations. Therefore, it is not pg_bulkload's goal to provide detailed data validation. Rather, pg_bulkload asumes that loaded data set is validated by separate means. If you're not in such situation, you should use COPY command in PostgreSQL.


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

install -m 755 bin/pg_bulkload                 %{buildroot}%{_bindir}/pg_bulkload
install -m 755 bin/postgresql                  %{buildroot}%{_bindir}/postgresql
install -m 755 lib/pg_bulkload.so              %{buildroot}%{_libdir}/pg_bulkload.so

install -m 644 lib/pg_bulkload.sql             %{buildroot}%{pg_contribdir}/pg_bulkload.sql
install -m 644 lib/uninstall_pg_bulkload.sql   %{buildroot}%{pg_contribdir}/uninstall_pg_bulkload.sql
install -m 644 lib/pg_bulkload.control         %{buildroot}%{pg_extensiondir}/pg_bulkload.control
install -m 644 lib/pg_bulkload--1.0.sql        %{buildroot}%{pg_extensiondir}/pg_bulkload--1.0.sql
install -m 644 lib/pg_bulkload--unpackaged--1.0.sql         %{buildroot}%{pg_extensiondir}/pg_bulkload--unpackaged--1.0.sql

# sample_*.ctl files are needed for rpm users.
# %{sname}-%{version} is the same path with "%setup -n"'s argument.
install -m 644 doc/sample_bin.ctl              %{buildroot}%{pg_contribdir}/sample_bin.ctl
install -m 644 doc/sample_csv.ctl              %{buildroot}%{pg_contribdir}/sample_csv.ctl

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

%clean
rm -rf %{buildroot}
rm -rf %{_libdir}/pgxs/src/backend/

%changelog
* Mon Jan 25 2016 - Amit Langote <langote_amit_f8@lab.ntt.co.jp> 3.1.9-1
- Update to pg_bulkload 3.1.9
* Mon Feb 16 2015 - Takashi OHNISHI <onishi_takashi_d5@lab.ntt.co.jp> 3.1.8-1
- Update to pg_bulkload 3.1.8
* Tue Jan 06 2015 - Takashi OHNISHI <onishi_takashi_d5@lab.ntt.co.jp> 3.1.7-1
- Update to pg_bulkload 3.1.7
* Fri Mar 23 2014 - Takashi OHNISHI <onishi_takashi_d5@lab.ntt.co.jp> 3.1.6-1
- Update to pg_bulkload 3.1.6
* Thu Dec 12 2013 - Takashi OHNISHI <onishi_takashi_d5@lab.ntt.co.jp> 3.1.5-1
- Update to pg_bulkload 3.1.5
* Thu Jul 25 2013 - Takashi OHNISHI <onishi_takashi_d5@lab.ntt.co.jp> 3.1.4-1
- Update to pg_bulkload 3.1.4
* Tue Apr 08 2013 - Takashi OHNISHI <onishi_takashi_d5@lab.ntt.co.jp> 3.1.3-1
- Update to pg_bulkload 3.1.3
* Thu Oct 25 2012 - Takashi OHNISHI <onishi_takashi_d5@lab.ntt.co.jp> 3.1.2-1
- Update to pg_bulkload 3.1.2
* Mon Jan 31 2011 - Kenji OTSUKA <otsuka.kenji@oss.ntt.co.jp> 3.0.1-1
- Update to pg_bulkload 3.0.1
* Thu Dec 01 2010 - Kenji OTSUKA <otsuka.kenji@oss.ntt.co.jp> 3.0.0-1
- Update to pg_bulkload 3.0.0 for PostgreSQL 9.0
* Tue Apr 28 2009 - Toru SHIMOGAKI <shimogaki.toru@oss.ntt.co.jp> 2.4.0-1
- Fix BUILD directory "%{sname}-%{version}
* Tue Apr 28 2009 - Toru SHIMOGAKI <shimogaki.toru@oss.ntt.co.jp> 2.4beta2-1
- No need to prepare nbtsort.c and pg_crc for rpm only, because
  o pg_bulkload source code includes nbtsort.c, and
  o pg_crc isn't used now because of changing the management of load status
    file protection logic.
* Thu Apr 24 2009 - Toru SHIMOGAKI <shimogaki.toru@oss.ntt.co.jp> 2.4beta1-2
- Use nbtsort.c and pg_crc.c directly.
* Thu Apr 23 2009 - Toru SHIMOGAKI <shimogaki.toru@oss.ntt.co.jp> 2.4beta1-1
- Initial packaging
