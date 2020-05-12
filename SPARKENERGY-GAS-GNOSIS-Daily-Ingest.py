##Northeast
start_ops >> start_Northeast
#meter_info
start_Northeast >> find_northeast_meter_info_file >> does_northeast_meter_info_file_exist >> northeast_meter_info_found >> t_unzip_northeast_meter_info >> northeast_meter_unzip
does_northeast_meter_info_file_exist >> northeast_meter_info_missing >> northeast_meter_info_email_unavailable_file >> t_unzip_yesterdays_northeast_meter_info >> t_move_yesterday_northeast_meter_info >> northeast_meter_unzip
northeast_meter_unzip >> t_move_northeast_meter_info >> t_add_partition_northeast_meter_info >> t_verify_partition_northeast_meter_info >> finish_Northeast
#meter_attribute
start_Northeast >> find_northeast_meter_attributes_file >> does_northeast_meter_attributes_file_exist >> northeast_meter_attributes_found >> t_unzip_northeast_meter_attributes >> northeast_attribute_unzip
does_northeast_meter_attributes_file_exist >> northeast_meter_attributes_missing >> northeast_meter_attributes_email_unavailable_file >> t_unzip_yesterday_northeast_meter_attributes >> t_move_yesterday_northeast_meter_attributes >> northeast_attribute_unzip
northeast_attribute_unzip >> t_move_northeast_meter_attributes >> t_add_partition_northeast_meter_attributes >> t_verify_partition_northeast_meter_attributes >> finish_Northeast
#scalar
start_Northeast >> find_northeast_scalar_file >> does_northeast_scalar_file_exist >> northeast_scalar_found >> t_unzip_northeast_scalar_usage >> northeast_scalar_unzip
does_northeast_scalar_file_exist >> northeast_scalar_missing >> northeast_scalar_email_unavailable_file >> t_unzip_yesterday_northeast_scalar_usage >> t_move_yesterday_northeast_scalar_usage >> northeast_scalar_unzip
northeast_scalar_unzip >> t_move_northeast_scalar_usage >> t_add_partition_northeast_scalar_usage >> t_verify_partition_northeast_scalar_usage >> finish_Northeast


finish_Northeast >> finish_ops


##West
start_ops >> start_West
#meter_info
start_West >> find_west_meter_info_file >> does_west_meter_info_file_exist >> west_meter_info_found >> t_unzip_west_meter_info >> west_meter_unzip
does_west_meter_info_file_exist >> west_meter_info_missing >> west_meter_info_email_unavailable_file >> t_unzip_yesterdays_west_meter_info >> t_move_yesterday_west_meter_info >> west_meter_unzip
west_meter_unzip >> t_move_west_meter_info >> t_add_partition_west_meter_info >> t_verify_partition_west_meter_info >> finish_West
#meter_attribute
start_West >> find_west_meter_attributes_file >> does_west_meter_attributes_file_exist >> west_meter_attributes_found >> t_unzip_west_meter_attributes >> west_attribute_unzip
does_west_meter_attributes_file_exist >> west_meter_attributes_missing >> west_meter_attributes_email_unavailable_file >> t_unzip_yesterday_west_meter_attributes >> t_move_yesterday_west_meter_attributes >> west_attribute_unzip
west_attribute_unzip >> t_move_west_meter_attributes >> t_add_partition_west_meter_attributes >> t_verify_partition_west_meter_attributes >> finish_West
#scalar
start_West >> find_west_scalar_file >> does_west_scalar_file_exist >> west_scalar_found >> t_unzip_west_scalar_usage >> west_scalar_unzip
does_west_scalar_file_exist >> west_scalar_missing >> west_scalar_email_unavailable_file >> t_unzip_yesterday_west_scalar_usage >> t_move_yesterday_west_scalar_usage >> west_scalar_unzip
west_scalar_unzip >> t_move_west_scalar_usage >> t_add_partition_west_scalar_usage >> t_verify_partition_west_scalar_usage >> finish_West

finish_West >> finish_ops
