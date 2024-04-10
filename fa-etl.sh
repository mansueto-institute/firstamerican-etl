#!/bin/bash

working_directory=/Users/claireboyd/internships/mansueto/firstamerican-etl/dev/36061

log_file_arg=/Users/claireboyd/internships/mansueto/firstamerican-etl/deployments/deploy_etl.log
input_dir_arg=$working_directory/

python fa-etl.py --input_dir $input_dir_arg --log_file $log_file_arg
