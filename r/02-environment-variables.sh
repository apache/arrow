#!/bin/bash

# set environment variables

source ~/.bashrc

if [[ $ARROW_HOME == "" ]]
then
 echo 'adding ARROW_HOME to .bashrc' 
 printf '\n#ARROW VARS\nexport ARROW_HOME=~/.arrow' | tee -a ~/.bashrc
else
 echo 'ARROW_HOME is already set'
fi

if [[ $LD_LIBRARY_PATH == "" ]]
then
 echo 'adding LD_LIBRARY_PATH to .bashrc'
 printf '\nexport LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}' | tee -a ~/.bashrc
else
 echo 'LD_LIBRARY_PATH is already set'
fi

if [[ $ARROW_R_DEV == "" ]]
then
 echo 'adding ARROW_R_DEV to .bashrc'
 printf '\nexport ARROW_R_DEV=true' | tee -a ~/.bashrc
else
 echo 'ARROW_R_DEV is already set'
fi

if [[ $PKG_CONFIG_PATH == "" ]]
then
 echo 'adding PKG_CONFIG_PATH to .bashrc'
 printf '\nexport PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig' | tee -a ~/.bashrc
else
 echo 'PKG_CONFIG_PATH is already set'
fi

if [[ $ARROW_WITH_LZ4 == "" ]]
then
 echo 'adding ARROW_WITH_LZ4 to .bashrc'
 printf '\nexport ARROW_WITH_LZ4=ON' | tee -a ~/.bashrc
else
 echo 'ARROW_WITH_LZ4 is already set'
fi

if [[ $ARROW_WITH_BROTLI == "" ]]
then
 echo 'adding ARROW_WITH_BROTLI to .bashrc'
 printf '\nexport ARROW_WITH_BROTLI=ON' | tee -a ~/.bashrc
else
 echo 'ARROW_WITH_BROTLI is already set'
fi

# verify env vars
source ~/.bashrc
echo $ARROW_HOME
echo $LD_LIBRARY_PATH
echo $ARROW_R_DEV
echo $PKG_CONFIG_PATH
echo $ARROW_WITH_LZ4
echo $ARROW_WITH_BROTLI

