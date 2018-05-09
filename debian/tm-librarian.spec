# RPM spec file for building three packages: python3-tm-librarian, tm-librarian, and tm-lfs.
# This file is referenced in gbp.conf or can be used "standalone".  It was started from
# "alien -gckr <package>.deb" but has been modified extensively.  The three files follow
# the Debian precedent.  The first package is all the Python scripts, and the following
# packages tm-librarian and tm-lfs and ToRMS and node configurations, respectively.

# This definition is about file locations.  "global" is a fixed assignment, now.
%global pathbase tm-librarian

Name:		python3-tm-librarian
Summary:	Python3 files for The Machine from HPE
Version:	1.35
Release:	3

# Buildroot aligns with debian/gbp.conf
License:	see /usr/share/doc/tm-librarian/copyright
Distribution:	RPM-based
Group:		System Environment/Daemons
Packager: 	Advanced Software Development, Ft. Collins CO
Vendor:		Hewlett Packard Enterprise
URL:		http://www.hpe.com/TheMachine
requires:	python3
buildarch:	noarch

%description
Executable and library code for tm-librarian and tm-lfs, both per-node and
Top of Rack Management Server (ToRMS)

# Taken from "alien -g".  The relative reference is to %buildroot
%define _rpmdir ../
%define _rpmfilename %%{NAME}-%%{VERSION}-%%{RELEASE}.%%{ARCH}.rpm
%define _unpackaged_files_terminate_build 0

# Override defaults that show up in $HOME.  In particular, since I don't
# have a source tarball in .../SOURCE, repoint things here.

%define _topdir %{getenv:PWD}

# Scriptlet wrappers cd here just before invoking %scripts:
# builddir == source
%define _builddir %{_topdir}

# Not sure why kicking _buildrootdir doesn't propagate to this
%define buildroot %{_topdir}/FSOVERLAY

# SLES and CentOS don't have this generic directory; they use the actual 
# Python version.  Since the programs are done via symlinks in /usr/bin
# just hardcode this for now. %{buildroot} == $RPM_BUILD_ROOT.  "define"
# defers to each runtime invocation, ie, deferred evaluation.

%define targetdir %{buildroot}/usr/lib/python3/dist-packages/%{pathbase}

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
# None of the packages have these sections
# %prep
# %setup
# %build

###########################################################################
# Main package python3-tm-librarian, just the files
%install

# Last thing the scriptlet wrapper did was a cd....

env | grep RPM | sort
/bin/pwd
ls -CF
echo PWD = %{PWD}
echo _builddir = %{_builddir}
echo _topdir = %{_topdir}
echo targetdir = %{targetdir}

if [ -z "$RPM_BUILD_ROOT" -o "$RPM_BUILD_ROOT" = "/" ]; then
	echo Bad RPM_BUILD_ROOT >&2
	exit 99
fi

if [ "$RPM_BUILD_ROOT" != "/" ]; then
	rm -rf $RPM_BUILD_ROOT
fi
mkdir -p %{targetdir}	# It's under $RPM_BUILD_ROOT, see the define

# I think I'm at the top of the git repo...

cp -av src/*.py %{targetdir}
cp -avr *.py configfiles docs systemd templates tests %{targetdir}

exit 0

###########################################################################
%post -n tm-librarian

###########################################################################
%post -n tm-lfs

