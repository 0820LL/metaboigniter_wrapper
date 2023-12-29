#!/usr/bin/env python3

import argparse
import time
import os
import json
from metaboigniter_step import monitor_execution
from metaboigniter_report import make_json_report


def make_csv_file(analysis_path, patient_name, sample_type_l, sample_name_l, sample_file_l) -> str:
    csv_file = '{}/samplesheet.csv'.format(analysis_path)
    csv_header = 'sample,level,type,msfile'
    csv_content = ''
    # for index in range(0, len(sample_file_l)):
    #     csv_content += '{},{},auto\n'.format(
    #         sample_name_l[index], sample_file_l[index])
    # this is just a demo
    csv_content += 'test1,MS1,normal,/data_5t/lilin/products/metaboigniter/test_2.0/mzML_POS_Quant/X2_Rep1.mzML\n'
    csv_content += 'test2,MS1,normal,/data_5t/lilin/products/metaboigniter/test_2.0/mzML_POS_Quant/X3_Rep1.mzML\n'
    csv_content += 'test3,MS1,normal,/data_5t/lilin/products/metaboigniter/test_2.0/mzML_POS_Quant/X6_Rep1.mzML\n'
    csv_content += 'ID1,MS2,normal,/data_5t/lilin/products/metaboigniter/test_2.0/mzML_POS_ID/Pilot_MS_Control_2_peakpicked.mzML\n'
    csv_content += 'ID2,MS2,normal,/data_5t/lilin/products/metaboigniter/test_2.0/mzML_POS_ID/Pilot_MS_Pool_2_peakpicked.mzML\n'
    with open(csv_file, 'w') as csv_f:
        csv_f.write(csv_header + '\n')
        csv_f.write(csv_content[:-1])
    return csv_file


def make_params_file() -> str:
    pass


def steward(config_file_path, metaboigniter_path, send_message_script) -> None:
    start_time    = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    analysis_path = os.path.dirname(config_file_path)
    os.chdir(analysis_path)
    # get the parameters from the config.json file
    with open(config_file_path, 'r') as config_f:
        config_file_d = json.load(config_f)
    task_id = config_file_d['taskId']
    analysis_record_id = config_file_d['analysisRecordId']
    task_name = config_file_d['taskName']
    pipeline_name = config_file_d['pipeline']
    if 'patientId2' in config_file_d:
        patient_name = config_file_d['patientId2']
    else:
        patient_name = '--'
    # parameters got from the config.json file
    if 'identification' in config_file_d['parameterList']:
        identification = config_file_d['parameterList']['identification']
    else: 
        identification = True  # True or False
    if 'polarity' in config_file_d['parameterList']:
        polarity = config_file_d['parameterList']['polarity']
    else: 
        polarity = 'positive'  # positive or negative
    if 'ms2_collection_model' in config_file_d['parameterList']:
        ms2_collection_model = config_file_d['parameterList']['ms2_collection_model']
    else: 
        ms2_collection_model = 'separate'  # separate or paired
    if 'run_sirius' in config_file_d['parameterList']:
        run_sirius = config_file_d['parameterList']['run_sirius']
    else: 
        run_sirius = True  # True or False
    if 'sirius_split' in config_file_d['parameterList']:
        sirius_split = config_file_d['parameterList']['sirius_split']
    else: 
        sirius_split = True  # True or False
    if 'mgf_splitmgf_pyopenms' in config_file_d['parameterList']:
        mgf_splitmgf_pyopenms = config_file_d['parameterList']['mgf_splitmgf_pyopenms']
    else:
        mgf_splitmgf_pyopenms = 100  # integer, default:1
    if 'run_ms2query' in config_file_d['parameterList']:
        run_ms2query = config_file_d['parameterList']['run_ms2query']
    else: 
        run_ms2query = False  # True or False
    if 'requantification' in config_file_d['parameterList']:
        requantification = config_file_d['parameterList']['requantification']
    else: 
        requantification = True   # True or False
    # the sample information from the config.json file
    sample_name_l = []
    sample_type_l = []
    sample_file_l = []
    sample_id_l = []
    for sample in config_file_d['taskSampleList']:
        sample_name_l.append(sample['sampleName'])  # the sample name
        # the sample type, [ tumor | normal ]
        sample_type_l.append(sample['sampleType'].lower())
        # the fastq file including R1 and R2
        sample_file_l.append('{},{}'.format(sample['read1'], sample['read2']))
        sample_id_l.append(sample['sampleId'])
    os.chdir(analysis_path)
    csv_file = make_csv_file(analysis_path, patient_name,
                             sample_type_l, sample_name_l, sample_file_l)
    # make the parms.json file
    params_d = {
        'identification'       : identification,
        'polarity'             : polarity,
        'ms2_collection_model' : ms2_collection_model,
        'run_sirius'           : run_sirius,
        'sirius_split'         : sirius_split,
        'mgf_splitmgf_pyopenms': int(mgf_splitmgf_pyopenms),
        'run_ms2query'         : run_ms2query,
        'requantification'     : requantification,
        'input'                : csv_file,
        'outdir'               : 'results'
    }
    params_file_path = '{}/params.json'.format(analysis_path)
    with open(params_file_path, 'w') as params_f:
        json.dumps(params_d, ensure_ascii=False, fp=params_f, indent=4)
    metaboigniter_command = 'nextflow run -offline -profile singularity -bg -params-file {} {} >> run_metaboigniter.log'.format(
        params_file_path, metaboigniter_path)
    return_value = os.system(metaboigniter_command)
    with open('metaboigniter_command.txt', 'w') as metaboigniter_command_f:
        metaboigniter_command_f.write(metaboigniter_command + '\n')
        metaboigniter_command_f.write('return value:{}\n'.format(str(return_value)))
    # to monitor the pipeline execution status and send messages
    monitor_execution_params_d = {
        'analysis_path'       : analysis_path,
        'send_message_script' : send_message_script,
        'return_value'        : return_value,
        'start_time'          : start_time,
        'task_id'             : task_id,
        'analysis_record_id'  : analysis_record_id,
        'identification'      : identification,
        'polarity'            : polarity,
        'ms2_collection_model': ms2_collection_model,
        'run_sirius'          : run_sirius,
        'run_ms2query'        : run_ms2query,
        'requantification'    : requantification,
    }
    monitor_execution(monitor_execution_params_d)
    # to generate the report json file
    make_json_report_params_d = {
        'analysis_path'       : analysis_path,
        'send_message_script' : send_message_script,
        'task_id'             : task_id,
        'analysis_record_id'  : analysis_record_id,
        'identification'      : identification,
        'polarity'            : polarity,
        'ms2_collection_model': ms2_collection_model,
        'run_sirius'          : run_sirius,
        'run_ms2query'        : run_ms2query,
        'requantification'    : requantification,
    }
    make_json_report(make_json_report_params_d)


def main() -> None:
    parser = argparse.ArgumentParser(
        description='transfer the config.json to csv file; invoke the rnaseq pipeline; feedback the information to front end')
    parser.add_argument('--cfp', required=True,
                        help='the full path for the config.json file')
    parser.add_argument('--metaboigniter_path', required=True,
                        help='the full path for rnaseq')
    parser.add_argument('--send_message_script', required=True,
                        help='the full path for the shell script: sendMessage.sh')
    args = parser.parse_args()
    config_file_path = args.cfp
    rnaseq_path = args.rnaseq_path
    send_message_script = send_message_script
    steward(config_file_path, rnaseq_path, send_message_script)


if __name__ == '__main__':
    main()
