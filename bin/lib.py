"""
coding=utf-8
"""
import logging
import os
import re
import subprocess

import pandas
import yaml

from bin import pdf2text

CONFS = None

AVAILABLE_EXTENSIONS = {'.csv', '.doc', '.docx', '.eml', '.epub', '.gif', '.htm', '.html', '.jpeg', '.jpg', '.json',
                        '.log', '.mp3', '.msg', '.odt', '.ogg', '.pdf', '.png', '.pptx', '.ps', '.psv', '.rtf', '.tff',
                        '.tif', '.tiff', '.tsv', '.txt', '.wav', '.xls', '.xlsx'}


def load_confs(confs_path='../confs/config.yaml'):
    # TODO Docstring
    global CONFS

    if CONFS is None:
        try:
            CONFS = yaml.load(open(confs_path))
        except IOError:
            confs_template_path = confs_path + '.template'
            logging.warn(
                'Confs path: {} does not exist. Attempting to load confs template, '
                'from path: {}'.format(confs_path, confs_template_path))
            CONFS = yaml.load(open(confs_template_path))
    return CONFS


def get_conf(conf_name):
    return load_confs()[conf_name]


def archive_dataset_schemas(step_name, local_dict, global_dict):
    logging.info('Archiving data set schema(s) for step name: {}'.format(step_name))

    # Reference variables
    data_schema_dir = get_conf('data_schema_dir')
    schema_output_path = os.path.join(data_schema_dir, step_name + '.csv')
    schema_agg = list()

    env_variables = dict()
    env_variables.update(local_dict)
    env_variables.update(global_dict)

    # Filter down to Pandas DataFrames
    data_sets = filter(lambda x: type(x[1]) == pandas.DataFrame, env_variables.items())
    data_sets = dict(data_sets)

    for (data_set_name, data_set) in data_sets.items():
        # Extract variable names
        logging.info('Working data_set: {}'.format(data_set_name))

        local_schema_df = pandas.DataFrame(data_set.dtypes, columns=['type'])
        local_schema_df['data_set'] = data_set_name

        schema_agg.append(local_schema_df)

    # Aggregate schema list into one data frame
    agg_schema_df = pandas.concat(schema_agg)

    # Write to file
    agg_schema_df.to_csv(schema_output_path, index_label='variable')


def term_count(string_to_search, term):
    try:
        regular_expression = re.compile(term, re.IGNORECASE)
        result = re.findall(regular_expression, string_to_search)
        return len(result)
    except Exception:
        logging.error('Error occurred during regex search')
        return 0


def term_match(string_to_search, term):
    try:
        regular_expression = re.compile(term, re.IGNORECASE)
        result = re.findall(regular_expression, string_to_search)
        if len(result) > 0:
            return result[0]
        else:
            return None
    except Exception:
        logging.error('Error occurred during regex search')
        return None

def convert_pdf(f):

    # Create intermediate output file
    # TODO Is this a desirable feature? Could this be replaced with a tempfile or fake file?
    output_filename = os.path.basename(os.path.splitext(f)[0]) + '.txt'
    output_filepath = os.path.join('..', 'data', 'output', output_filename)
    logging.info('Writing text from {} to {}'.format(f, output_filepath))

    # Convert pdf to text, placed in intermediate output file
    pdf2text.main(args=[f, '--outfile', output_filepath])

    # Return contents of intermediate output file
    return open(output_filepath).read()



