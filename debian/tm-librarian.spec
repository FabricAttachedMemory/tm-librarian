# RPM spec file for building three packages: python3-tm-librarian, tm-librarian, and tm-lfs.
# This file is referenced in gbp.conf or can be used "standalone".  It was started from
# "alien -gckr <package>.deb" but has been modified extensively.  The three files follow
# the Debian precedent.  The first package is all the Python scripts, and the following
# packages tm-librarian and tm-lfs and ToRMS and node configurations, respectively.

# This definition is about file locations.  "global" is a fixed assignment, now.
%global pathbase tm_librarian

Name:		python3-tm-librarian
Summary:	Python3 files for The Machine from HPE
Version:	1.35
Release:	3

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

# By default (/usr/lib/rpm/macros) this is $HOME/rpmbuild.  In particular,
# since I don't have a source tarball in .../SOURCE, repoint things here 
# (top of git repo).  Kicking buildroot[dir] in here does not propagate 
# into the %files section, evaluations must be occurring in a weird order.
# Setting these dirs does work from the command line:
# --buildroot $PWD/BUILDROOT (keep the legacy name).  It will be deleted
# unless --noclean is also specified.  --buildroot is not needed here.

%define _topdir %{getenv:PWD}

# Scriptlet wrappers cd here (aka RPM_BUILD_DIR) just before %scripts:
# builddir == source material after raw source has been "built", a noop here.
# Do NOT run py3compile here as the build system Python != delivery system.

%define _builddir "%{_topdir}"

# SLES12.  CentOS does something else.
%define usrlocalman8 /usr/local/man/man8

# SLES and CentOS don't have this generic directory; they use the actual 
# Python version.  Since the programs are done via symlinks in /usr/bin
# just hardcode this for now.  "define" is per-use deferred evaluation,
# vs %global.

%define distpackages /usr/lib/python3/dist-packages/%{pathbase}
%define targetdir %{buildroot}%{distpackages}

# Taken (mostly) from "alien -g".  The relative reference is to %buildrootdir
# which is one level up from the --buildroot cmdline option.  In this
# setup it's the same as topdir.  Hmmm maybe that's not such a hot idea...

%define _rpmdir ./RPMS
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
%prep

# This is not really needed until the %files stage.  rpmbuild will make
# all the dirs by default in $HOME/rpmbuild/(SOURCE, BUILD, etc) but only
# makes BUILDROOT if things are changed via --buildroot.

mkdir -p %{_rpmdir}	# If no --buildir is used, this is automagic

###########################################################################
# %setup
# %build

###########################################################################
# Main package python3-tm-librarian, just the files.

%install

# Last thing the scriptlet wrapper did was a cd $RPM_BUILD_DIR

if true; then
	env | grep RPM | sort
	/bin/pwd
	ls -CF
	echo PWD = %{PWD}
	echo _builddir = %{_builddir}
	echo _topdir = %{_topdir}
	echo targetdir = %{targetdir}
fi

# sync sync sync, aka legacy paranoia
if [ -z "$RPM_BUILD_ROOT" -o "$RPM_BUILD_ROOT" = "/" ]; then
	echo Bad RPM_BUILD_ROOT >&2
	exit 99
fi
rm -rf $RPM_BUILD_ROOT
mkdir -p %{targetdir}	# It's under $RPM_BUILD_ROOT, see the define

# PWD == top of the git repo

cp -ar src/*.py configfiles templates tests %{targetdir}

MANDIR=$RPM_BUILD_ROOT%{usrlocalman8}
mkdir -p $MANDIR
cp -ar docs/*.8 $MANDIR

###########################################################################
# Alien spec delineated each file.  I'm lazy.

%files
%defattr(-, root, root)
%{distpackages}

###########################################################################
# Stolen from alien spec

%post

if which py3compile >/dev/null 2>&1; then
	cd %{distpackages}/..
        py3compile -p %{pathbase} -V 3.2-
fi

###########################################################################
# Stolen from alien spec

%preun
find "%{distpackages}" -type d -name __pycache__ | while read D; do
	rm -rf "$D"
done

for UNIT in tm-lfs tm-librarian tm-lmp; do
    for ACTION in stop disable; do
        systemctl $ACTION $UNIT || echo "Couldn't $ACTION $UNIT" >&2
    done
done
systemctl daemon-reload || echo "systemctl daemon-reload failed" >&2

if which py3clean >/dev/null 2>&1; then
	cd %{distpackages}/..
        py3clean -p %{pathbase} -V 3.2-
else
	# Big messy inline perl mess.  JSN.
	:
fi

###########################################################################
%files -n tm-librarian

# Sure, give them all the files
%{usrlocalman8}

###########################################################################
%post -n tm-librarian

###########################################################################
%files -n tm-lfs

%{usrlocalman8}/tm-lfs.8

###########################################################################
%post -n tm-lfs

