#!/bin/bash

working_directory=/Users/claireboyd/internships/mansueto/firstamerican-etl/dev/36061

log_file_arg=/Users/claireboyd/internships/mansueto/firstamerican-etl/deployments/deploy_etl.log
input_dir_arg=$working_directory

# log_file_arg=/project/crberry/firstamerican-etl/deployments/deploy_etl.log
# input_dir_arg=/project/crberry/firstamerican-etl/deployments/national
annual_file_string_arg=Prop
value_history_file_string_arg=ValHist

python fa-etl.py --input_dir $input_dir_arg --log_file $log_file_arg --annual_file_string $annual_file_string_arg --value_history_file_string $value_history_file_string_arg
