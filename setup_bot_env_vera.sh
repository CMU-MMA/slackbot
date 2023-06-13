#!/bin/sh
# Just run this once to create the environment

echo "module purge"
module purge
echo "module load anaconda3/2020.07"
module load anaconda3/2020.07
# TC220118: cudatoolkit not immediately available on Vera
#echo "Loading cudatoolkit module"
#module load cudatoolkit

echo "source /opt/packages/anaconda3/etc/profile.d/conda.sh"
source /opt/packages/anaconda3/etc/profile.d/conda.sh

echo "conda env remove -n gw-bot"
conda env remove -n env4followups

echo "conda create -y --name gw-bot python=3.11"
conda create -y --name gw-bot python=3.11

echo "conda activate gw-bot"
conda activate gw-bot

# conda installs
echo "conda install -y -c anaconda numpy matplotlib=3.6 scipy pandas astropy joblib h5py shapely"
conda install -y -c anaconda numpy matplotlib=3.6 scipy pandas astropy joblib

echo "conda install -c astropy astroquery"
conda install -c astropy astroquery

#echo "conda install -c conda-forge voeventlib astropy-healpix python-ligo-lw ligo-segments ligo.skymap ffmpeg"
#conda install -c conda-forge voeventlib astropy-healpix python-ligo-lw ligo-segments ligo.skymap ffmpeg

echo "conda config --add channels conda-forge"
conda config --add channels conda-forge

echo "conda install astropy-healpix"
conda install astropy-healpix

echo "conda install voeventlib"
conda install voeventlib

echo "conda install -c conda-forge python-ligo-lw"
conda install -c conda-forge python-ligo-lw

echo "conda install python-ligo-lw ligo-segments ligo.skymap ffmpeg"
conda install -c conda-forge python-ligo-lw ligo-segments ligo.skymap ffmpeg

# pip installs
echo "which pip"
which pip
#echo "pip install python-ligo-lw"
#pip install python-ligo-lw
echo "pip install ligo.skymap==1.0.7 --no-cache-dir"
pip install ligo.skymap==1.0.7 --no-cache-dir
echo "pip install gwemopt"
pip install gwemopt
echo "pip install aiohttp"
pip install aiohttp
echo "pip intstall hop-client==0.8.0"
pip install hop-client==0.8.0
echo "pip install slack-sdk==3.21.1"
pip install slack-sdk==3.21.1
echo "pip install healpy==1.16.2"
pip install healpy==1.16.2
echo "pip install gwosc==0.7.1"
pip install sgwosc==0.7.1
echo "pip install gwpy==3.0.4"
pip install gwpy==3.0.4
echo "pip install pyOpenSSL"
pip install pyOpenSSL
echo "pip install versioneer==0.28"
pip install versioneer==0.28


#hop setup
hop auth locate
hop auth add 
#username = oconnorb-8dc3d960
#password = WkAJf6Cdg6ay8Ew9PL44b17tUmq7ESpr
hop subscribe kafka://kafka.scimma.org/igwn.gwalert


echo "conda deactivate"
conda deactivate