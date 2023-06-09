#!/bin/bash

# set up HPC environment
module purge
module load anaconda3/2020.07

# activate conda environment
source /opt/packages/anaconda3/etc/profile.d/conda.sh
conda deactivate
condaDIR="/hildafs/projects/phy220048p/oconnorb/.conda/gw-bot"

conda activate $condaDIR
pythonPATH="$condaDIR/bin/python"