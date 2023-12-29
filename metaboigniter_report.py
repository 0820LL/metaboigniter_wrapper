#!/usr/bin/env python3

import os
from metaboigniter_funcs import send_json_message

def make_json_report(make_json_report_params_d) -> None:
    analysis_path        = make_json_report_params_d['analysis_path']
    send_message_script  = make_json_report_params_d['send_message_script']
    task_id              = make_json_report_params_d['task_id']
    analysis_record_id   = make_json_report_params_d['analysis_record_id']
    identification       = make_json_report_params_d['identification']
    polarity             = make_json_report_params_d['polarity']
    ms2_collection_model = make_json_report_params_d['ms2_collection_model']
    run_sirius           = make_json_report_params_d['run_sirius']
    run_ms2query         = make_json_report_params_d['run_ms2query']
    requantification     = make_json_report_params_d['requantification']
    os.chdir(analysis_path)
    report_dict = {
        'status'          : 'Pass',
        'pipelineName'    : 'metaboigniter',
        'taskId'          : task_id,
        'analysisRecordId': analysis_record_id,
        'error'           : 0,
        'taskName'        : 'Report',
        'data'            : []
    }
    report_main_results_dict = {
        'sort'     : 1,
        'title'    : '分析结果下载查看',
        'subtitle1': '',
        'memo'     : '',
        'text'     : [
            {
                'sort'   : 1,
                'title'  : 'quantification分析结果下载查看',
                'content': 'quantification分析结果:#&{}/results/TABLE_OUTPUT/output_quantification_Linked_data.tsv'.format(analysis_path),
                'memo'   : '',
                'preDes' : '',
                'postDes': ''
            },
            {
                'sort'   : 2,
                'title'  : 'indentification(sirius)分析结果下载查看',
                'content': 'indentification(sirius)分析结果:#&{}/results/TABLE_OUTPUT/output_sirius_Linked_data.tsv'.format(analysis_path),
                'memo'   : '',
                'preDes' : '',
                'postDes': ''
            },
            {
                'sort'   : 3,
                'title'  : 'indentification(ms2query)分析结果下载查看',
                'content': 'indentification(ms2query)分析结果:#&{}/results/TABLE_OUTPUT/output_ms2query_Linked_data.tsv'.format(analysis_path),
                'memo'   : '',
                'preDes' : '',
                'postDes': ''
            },
        ]
    }
    report_dict['data'].append(report_main_results_dict)

    report_reference_dict = {
        'sort' : 2,
        'title': '分析工具及参考文献',
        'text' : [
            {
                "sort"   : 1,
                "content": "[1] Di Tommaso P, Chatzou M, Floden EW, Barja PP, Palumbo E, Notredame C. Nextflow enables reproducible computational workflows. Nat Biotechnol. 2017 Apr 11;35(4):316-319. doi: 10.1038/nbt.3820. PubMed PMID: 28398311.",
                "memo"   : ""
            },
            {
                "sort"   : 2,
                "content": "[2] Ewels PA, Peltzer A, Fillinger S, Patel H, Alneberg J, Wilm A, Garcia MU, Di Tommaso P, Nahnsen S. The nf-core framework for community-curated bioinformatics pipelines. Nat Biotechnol. 2020 Mar;38(3):276-278. doi: 10.1038/s41587-020-0439-x. PubMed PMID: 32055031.",
                "memo"   : ""
            },
            {
                "sort"   : 3,
                "content": "[3] Röst, H. L., Sachsenberg, T., Aiche, S., Bielow, C., Weisser, H., Aicheler, F., ... & Kohlbacher, O. (2016). OpenMS: a flexible open-source software platform for mass spectrometry data analysis. Nature Methods, 13(9), 741-748",
                "memo"   : ""
            },
            {
                "sort"   : 4,
                "content": "[4] Dührkop, K., Fleischauer, M., Ludwig, M., Aksenov, A. A., Melnik, A. V., Meusel, M., ... & Böcker, S. (2019). SIRIUS 4: a rapid tool for turning tandem mass spectra into metabolite structure information. Nature Methods, 16(4), 299-302.",
                "memo"   : ""
            }
        ]
    }
    report_dict['data'].append(report_reference_dict)

    for file in os.listdir('{}/results/pipeline_info'.format(analysis_path)):
        if file.startswith('execution_report'):
            execution_report_file_path = '{}/results/pipeline_info/{}'.format(analysis_path, file)
        if file.startswith('execution_timeline'):
            execution_timeline_file_path = '{}/results/pipeline_info/{}'.format(analysis_path, file)
        if file.startswith('pipeline_dag'):
            pipeline_dag_file_path = '{}/results/pipeline_info/{}'.format(analysis_path, file)
    report_download_dict = {
        'sort' : 3,
        'title': '可视化报告下载',
        'text' : [
            {
                'sort'   : 1,
                'title'  : '分析流程运行监控报告',
                'content': '分析流程运行监控报告下载：#&{}'.format(execution_report_file_path),
                'postDes': '',
                'memo'   : '',
                'preDes' : '详细展示了各分析模块的资源使用情况'
            },
            {
                'sort'   : 2,
                'title'  : '分析流程运行时间报告',
                'content': '分析流程运行时间监控报告下载：#&{}'.format(execution_timeline_file_path),
                'postDes': '',
                'memo'   : '',
                'preDes' : '详细展示了各分析模块的运行时间情况'
            },
            {
                'sort'   : 3,
                'title'  : '分析模块逻辑关系',
                'content': '分析流程中各模块的逻辑关系报告下载：#&{}'.format(pipeline_dag_file_path),
                'postDes': '',
                'memo'   : '',
                'preDes' : '详细展示了各分析模块间的逻辑关系'
            }

        ], 
        'memo'     : '',
        'subtitle1': '',
        'subtitle2': ''
    }
    report_dict['data'].append(report_download_dict)
    send_json_message(analysis_path, send_message_script, report_dict, 'Report.json')