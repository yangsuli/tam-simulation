"""
helper functions
"""


def get_all_stage_name_list():
    all_stg_name_list = \
        ['c_req_handle_stg', 'c_respond_stg', 'msg_in_stg', 'msg_out_stg', 'request_response_stg',
         # main stages to function in TAD (need for all request), below is 'proc_stg'
         'read_stg', 'mutation_stg', 'read_repair_stg', 'replicate_on_write_stg', 'gossip_stg', 'anti_entropy_stg',
         'migration_stg', 'flush_writer_stg', 'misc_stg', 'internal_response_stg', 'hinted_handoff_stg',
         'memory_meter_stg', 'pend_range_calculator_stg', 'commit_log_archiver_stg', 'anti_entropy_session_stg']
    return all_stg_name_list


def guess_stage_name(guess_input_str):
    possible_str_list = []
    for stg_name in get_all_stage_name_list():
        if guess_input_str in stg_name:
            possible_str_list.append(stg_name)
    if len(possible_str_list) > 0:
        return min(possible_str_list, key=len)
    raise RuntimeError('Fail to guess for input: ' + guess_input_str)
