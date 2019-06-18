#!/bin/bash
set -x

# - rmdir /s /Q C:\OpenSSL-Win32 C:\OpenSSL-Win64
pacman --noconfirm -Rcsu mingw-w64-{i686,x86_64}-toolchain gcc pkg-config
pacman --noconfirm -Scc
pacman --noconfirm -Syyu
pacman --noconfirm --needed -S git base-devel binutils

# Install core build stuff
pacman --noconfirm --needed -S mingw-w64-{i686,x86_64}-{crt,winpthreads,gcc,libtre,pkg-config,xz}

cd $APPVEYOR_BUILD_FOLDER
makepkg-mingw --noconfirm --noprogressbar --skippgpcheck --nocheck --syncdeps --rmdeps --cleanbuild
