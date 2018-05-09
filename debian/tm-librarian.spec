# RPM spec file for building three packages: python3-tm-librarian, tm-librarian, and tm-lfs.
# This file is referenced in gbp.conf or can be used "standalone".  It was started from
# "alien -gckr <package>.deb" but has been modified extensively.  The three files follow
# the Debian precedent.  The first package is all the Python scripts, and the following
# packages tm-librarian and tm-lfs and ToRMS and node configurations, respectively.

Name:		python3-tm-librarian
Summary:	Python3 files for The Machine from HPE
Version:	1.35
Release:	3

# Buildroot aligns with debian/gbp.conf
Buildroot:	/tmp/gbp4hpe/tm-librarian-%{version}
License:	see /usr/share/doc/tm-librarian/copyright
Distribution:	RPM-based
Group:		System Environment/Daemons
Packager: 	Advanced Software Development, Ft. Collins CO
Vendor:		Hewlett Packard Enterprise
URL:		http://www.hpe.com/TheMachine
requires:	python3

%description
Executable and library code for tm-librarian and tm-lfs, both per-node and
Top of Rack Management Server (ToRMS)

# Taken from "alien -g".  The relative reference is to %buildroot
%define _rpmdir ../
%define _rpmfilename %%{NAME}-%%{VERSION}-%%{RELEASE}.%%{ARCH}.rpm
%define _unpackaged_files_terminate_build 0

###########################################################################

%package -n tm-librarian

Summary:	Librarian for The Machine from HPE
Group:		System Environment/Daemons
Requires:	%{name}

%description -n tm-librarian
Database constructor, checker, and central daemon for the Librarian
File System (LFS) suite for The Machine from Hewlett Packard Enterprise.
The LFS Management Protocol (LMP) is also configured with this package.

###########################################################################

%package -n tm-lfs

Summary:	Librarian for The Machine from HPE
Group:		System Environment/Daemons
Requires:	%{name}

%description -n tm-lfs
Librarian File System (LFS) daemon for each node in The Machine.  It needs
to connect with a Librarian cental daemon.

###########################################################################
# %prep
# %setup
# %build

###########################################################################
%install

if [ -z "$RPM_BUILD_ROOT" -o "$RPM_BUILD_ROOT" = "/" ]; then
	echo Bad RPM_BUILD_ROOT >&2
	exit 99
fi

if [ "$RPM_BUILD_ROOT" != "/" ]; then
	rm -rf $RPM_BUILD_ROOT
fi
mkdir -p $RPM_BUILD_ROOT

