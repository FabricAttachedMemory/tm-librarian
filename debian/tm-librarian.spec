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

License:	GPLv2
Distribution:	RPM-based
Group:		System Environment/Daemons
Packager: 	Advanced Software Development, Ft. Collins CO
Vendor:		Hewlett Packard Enterprise
URL:		http://www.github.com/FabricAttachedMemory
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

Summary:	Librarian daemon for The Machine from HPE
Group:		System Environment/Daemons
Requires:	%{name}

%description -n tm-librarian
Database constructor, checker, and central daemon for the Librarian
File System (LFS) suite for The Machine from Hewlett Packard Enterprise.
The LFS Management Protocol (LMP) is also configured with this package.

###########################################################################

%package -n tm-lfs

Summary:	LFS per-node daemon for The Machine from HPE
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

set -u

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

#--------------------------------------------------------------------------
# Other files.  Define a (global) macro for use here and in %files/
# Man pages, SLES12 style.  CentOS does something else.

%define usrlocalman8 /usr/local/man/man8
MANDIR=$RPM_BUILD_ROOT%{usrlocalman8}
mkdir -p $MANDIR
cp -ar docs/*.8 $MANDIR

# Default config files for systemd scripts.  SLES does indeed have this.

%define etcdefault /etc/default
ETCDEFAULT=$RPM_BUILD_ROOT%{etcdefault}
mkdir -p $ETCDEFAULT
for F in tm-lfs tm-librarian tm-lmp; do
	cp -a systemd/$F $ETCDEFAULT
done

# Systemd script files, different from Debian's /lib/systemd/system
%define systemdsystem /usr/lib/systemd/system
SYSTEMDSYSTEM=$RPM_BUILD_ROOT%{systemdsystem}	# not RPM_BUILD_DIR
mkdir -p $SYSTEMDSYSTEM
for F in tm-lfs tm-librarian tm-lmp; do
	cp -a systemd/$F.service $SYSTEMDSYSTEM
done

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
%post -n tm-librarian

ln -sf %{distpackages}/fsck_lfs.py /usr/bin/fsck_lfs
ln -sf %{distpackages}/book_register.py /usr/bin/tm-book-register
ln -sf %{distpackages}/librarian.py /usr/bin/tm-librarian
ln -sf %{distpackages}/lmp.py /usr/bin/tm-lmp

systemctl daemon-reload || echo "systemctl daemon-reload failed" >&2

systemctl enable tm-librarian tm-lmp
[ -f /var/hpetm/librarian.db ] && systemctl start tm-librarian tm-lmp || true

#--------------------------------------------------------------------------
%preun -n tm-librarian

systemctl stop tm-librarian tm-lmp
systemctl disable tm-librarian tm-lmp

#--------------------------------------------------------------------------
%postun -n tm-librarian

for f in fsck_lfs tm-book-register tm-librarian tm-lmp; do
	unlink /usr/bin/$f || true
done

#--------------------------------------------------------------------------
%files -n tm-librarian

# Sure, give them all the man pages
%{usrlocalman8}

%config %{etcdefault}/tm-librarian
%config %{etcdefault}/tm-lmp

%{systemdsystem}/tm-librarian.service
%{systemdsystem}/tm-lmp.service

%{distpackages}/templates

# Not the /usr/bin files, see %post

###########################################################################
%post -n tm-lfs

ln -sf %{distpackages}/lfs_fuse.py /usr/bin/tm-lfs

systemctl daemon-reload || echo "systemctl daemon-reload failed" >&2

systemctl enable tm-lfs
systemctl start tm-lfs

#--------------------------------------------------------------------------
%preun -n tm-lfs

systemctl stop tm-lfs
systemctl disable tm-lfs

#--------------------------------------------------------------------------
%postun -n tm-lfs

for f in tm-lfs; do
	unlink /usr/bin/$f || true
done

#--------------------------------------------------------------------------
%files -n tm-lfs

%{usrlocalman8}/tm-lfs.8

%config %{etcdefault}/tm-lfs

%{systemdsystem}/tm-lfs.service

# Not the /usr/bin files, see %post
