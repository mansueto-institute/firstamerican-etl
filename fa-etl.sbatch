#!/bin/bash

#SBATCH --job-name=firstamerican_etl_job
#SBATCH --partition=caslake
#SBATCH --nodes=1
#SBATCH --ntasks=32
#SBATCH --mem=175000
#SBATCH --output=/project/crberry/firstamerican-etl/deployments/deploy_etl.out
#SBATCH --error=/project/crberry/firstamerican-etl/deployments/deploy_etl.err
#SBATCH --mail-type=ALL
#SBATCH --mail-user=ckboyd@rcc.uchicago.edu
#SBATCH --time=36:00:00
#SBATCH --account=pi-crberry

module load python/anaconda-2022.05

# CREATE CONDA ENVIRONMENT IF NOT ALREADY CREATED
# conda create --name fa_etl_env python=3.11.5 --yes
# source activate fa_etl_env
# conda install -c conda-forge polars --yes
# conda install -c conda-forge pyarrow --yes
# conda install psutil

source activate fa_etl_env

log_file_arg=/project/crberry/firstamerican-etl/deployments/deploy_etl.log
input_dir_arg=/project/crberry/firstamerican-etl/deployments/national
annual_file_string_arg=Annual
value_history_file_string_arg=ValueHistory

python fa-etl.py --input_dir $input_dir_arg --log_file $log_file_arg --annual_file_string $annual_file_string_arg --value_history_file_string $value_history_file_string_arg


