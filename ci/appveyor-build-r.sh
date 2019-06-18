#!/bin/bash
set -x

pacman --noconfirm -Rcsu mingw-w64-{i686,x86_64}-toolchain gcc pkg-config

wget https://raw.githubusercontent.com/r-windows/rtools-backports/master/pacman.conf
cp -f pacman.conf /etc/pacman.conf

pacman --noconfirm -Scc
pacman --noconfirm -Syyu
pacman --noconfirm --needed -S git base-devel binutils

# Install core build stuff
pacman --noconfirm --needed -S mingw-w64-{i686,x86_64}-{crt,winpthreads,gcc,libtre,pkg-config,xz}

#  Force static linking
rm -f /mingw32/lib/*.dll.a
rm -f /mingw64/lib/*.dll.a
export PKG_CONFIG="/${MINGW_INSTALLS}/bin/pkg-config --static"

cd $APPVEYOR_BUILD_FOLDER
makepkg-mingw --noconfirm --noprogressbar --skippgpcheck --nocheck --syncdeps --rmdeps --cleanbuild
