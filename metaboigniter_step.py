#!/usr/bin/env python3

import os
import time
from metaboigniter_funcs import send_json_message


def monitor_execution(monitor_execution_params_d) -> None:
    analysis_path        = monitor_execution_params_d['analysis_path']
    send_message_script  = monitor_execution_params_d['send_message_script']
    return_value         = monitor_execution_params_d['return_value']
    start_time           = monitor_execution_params_d['start_time']
    task_id              = monitor_execution_params_d['task_id']
    analysis_record_id   = monitor_execution_params_d['analysis_record_id']
    identification       = monitor_execution_params_d['identification']
    polarity             = monitor_execution_params_d['polarity']
    ms2_collection_model = monitor_execution_params_d['ms2_collection_model']
    run_sirius           = monitor_execution_params_d['run_sirius']
    run_ms2query         = monitor_execution_params_d['run_ms2query']
    requantification     = monitor_execution_params_d['requantification']
    os.chdir(analysis_path)
    step_dict = {
        'tTaskId'         : task_id,
        'analysisRecordId': analysis_record_id,
        'pipelineName'    : 'metaboigniter',
        'analysisStatus'  : '',
        'startDate'       : start_time,
        'endDate'         : '',
        'error'           : 0,
        'taskName'        : 'Step'
    }
# start        0  1  1  1  1  1  流程开始
# pre          0  0  1  1  1  1  quantification
# align        0  0  0  1  1  1  identification(sirius)
# post         0  0  0  0  1  1  identification（ms2query）
# multiqc      0  0  0  0  0  1  分析结束
# step_flag    0  1  2  3  4  5
# step_flag    0  流程开始
# step_flag    1  quantification
# step_flag    2  identification(sirius)
# step_flag    3  identification（ms2query）
# step_flag    4  分析结束

    step_flag = 0  # 流程开始
    # send the message of step_0
    time.sleep(60)
    step_dict['analysisStatus'] = '流程开始'
    step_file_name = 'step_start.json'
    if return_value == 0 and os.path.exists('{}/results'.format(analysis_path)):
        step_dict['endDate'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        step_dict['error']   = 0
        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
        step_flag = 1
    else:
        step_dict['endDate'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        step_dict['error']   = 1
        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
        exit('metaboigniter startup failed')
    # send the message of step_x
    while True:
        if 'pipeline_info' in os.listdir(os.getcwd() + '/results'):
            execution_trace_file += '/pipeline_info'
            for file in os.listdir(execution_trace_file):
                if file.startswith('execution_trace'):
                    execution_trace_file += '/{}'.format(file)
                    break
            if 'execution_trace' in execution_trace_file:
                break
        time.sleep(60)
    while True:
        # if cancel.txt or Cancel.txt is found, kill the pipeline and exit
        if os.path.exists('{}/cancel.txt'.format(analysis_path)) or os.path.exists('{}/Cancel.txt'.format(analysis_path)):
            step_dict['error'] = 2
            step_dict['endDate'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            if os.path.exists('{}/.nextflow.pid'.format(analysis_path)):
                with open('{}/.nextflow.pid'.format(analysis_path)) as f:
                    os.system('kill {}'.format(f.read().strip('/n')))
            send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
            exit('The analysis was cancelled')
        with open(execution_trace_file, encoding = 'UTF-8') as trace_f:
            for line in trace_f:
                if step_flag == 1:  #  quantification
                    step_file_name = 'step_quantification.json'
                    if ('QUANTIFICATION:OPENMS_MAPALIGNERPOSECLUSTERINGMZML ' in line):
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = 'quantification'
                        continue
                    if ('LINKER_REQ:OPENMS_FEATURELINKERUNLABELEDKD' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        step_flag = 2
                        continue
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_quantification')
                if step_flag == 2:  # identification(sirius)
                    step_file_name = 'step_identification_sirius.json'
                    if ('IDENTIFICATION:SIRIUSMAPPED:SIRIUS_SEARCH' in line):
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = 'identification(sirius)'
                        continue
                    if ('IDENTIFICATION:SIRIUSMAPPED:PYOPENMS_CONCTSVSIRIUS' in line ):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        step_flag = 3
                        continue
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_identification_sirius')
                if step_flag == 3:  # identification（ms2query）
                    step_file_name = 'step_identification_ms2query.json'
                    if ('IDENTIFICATION:MS2QUERYMAPPED:MS2QUERY_SEARCH' in line ):
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = 'identification(ms2query)'
                        continue
                    if 'IDENTIFICATION:MS2QUERYMAPPED:PYOPENMS_CONCTSVMS2QUERY' in line:
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        step_flag = 4
                        continue
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_identification_ms2query')
                if step_flag == 4:  # 分析结束
                    step_file_name= 'step_end.json'
                    if 'PYOPENMS_EXPORTIDENTIFICATION' in line:
                        step_dict['startDate']      = line.split('\t')[6][:-4]
                        step_dict['analysisStatus'] = '分析结束'
                        continue
                    if 'CUSTOM_DUMPSOFTWAREVERSIONS' in line:
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 0
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        break
                    if ('FAILED' in line) or ('ABORTED' in line):
                        step_dict['endDate'] = line.split('\t')[6][:-4]
                        step_dict['error']   = 1
                        send_json_message(analysis_path, send_message_script, step_dict, step_file_name)
                        exit('Failed or aborted in the step_end')
