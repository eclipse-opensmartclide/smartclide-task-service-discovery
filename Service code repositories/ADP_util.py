#
# Copyright (c) 2021 AIR Institute - Adrian Diarte Prieto
#
# This file is part of smartclide
# (see https://smartclide.eu/).
#
# This program is distributed under Eclipse Public License 2.0
# (see https://github.com/adriandpdev/Smartclide_apitest/blob/main/LICENSE.md)
#

import os
import pandas as pd
from os import scandir
from datetime import datetime


def generate_file(name, data):
    """
    Generate file with that configuration

    :param name: str
    :param data: List
    :return List:
    """
    # Create dataframe from json list
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tGenerando fichero >> {name}")
    df = pd.json_normalize(data=data)

    # Generate file
    df.to_csv('outputs/'+name, index=False, encoding='utf-8-sig')
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tListo! ")


def generate_file_del(name, data):
    """
    Generate file with that configuration

    :param name: str
    :param data: List
    :return List:
    """
    # Create dataframe from json list
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tGenerando fichero >> {name}")
    df = pd.json_normalize(data=data)
    if(len(df.columns) > 0):
        # Delete some columns
        try:
            df = df.drop(columns=del_columns, axis=1)
        except KeyError as kerr:
            error = kerr.replace("[","").split("]")
            del_columns2 = del_columns.pop(error)
            df = df.drop(columns=del_columns2, axis=1)
            print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \nError! ")
    # Generate file
    df.to_csv('outputs/'+name, index=False, encoding='utf-8-sig')
    print(f"[{str(datetime.now().strftime('%H:%M:%S'))}] \tListo! ")


def check_folder(folder):
    """
    Check if folder exists and create it if not exists

    :param folder: str
    """
    # Check if folder exists
    if not os.path.isdir('outputs/'+folder):
        # Create folder if not exists
        os.mkdir('outputs/'+folder)


def get_keywords(file):
    """
    Get all keywords from file

    :param file: str
    :return List:
    """
    # Initial var
    keywords = []

    f = open(f"{file}.txt")  # Open file
    for line in f:
        # Add kw to list
        keywords.append(line.rstrip('\n'))
    f.close()  # Close file
    return keywords

def get_files(route):
    """
    Get all files from route

    :param route: str
    :return List:
    """
    files = [obj.name for obj in scandir(route) if obj.is_file()]
    return files


# Vars
del_columns = [
    'connectActions',
    'cloneProtocol',
    'mirrors',
    'menuItems',
    'bitbucketActions',
    'activeMenuItem',
    'currentRepository.links.clone',
    'currentRepository.links.self.href',
    'currentRepository.links.avatar.href',
    'currentRepository.project.links.self.href',
    'currentRepository.project.links.avatar.href',
    'currentRepository.project.owner.links.self.href',
    'currentRepository.project.owner.links.avatar.href',
    'currentRepository.project.workspace.links.self.href',
    'currentRepository.project.workspace.links.avatar.href',
    'currentRepository.project.is_private',
    'currentRepository.owner.links.self.href',
    'currentRepository.owner.links.avatar.href',
    'currentRepository.owner.is_active',
    'currentRepository.owner.department',
    'currentRepository.owner.has_2fa_enabled',
    'currentRepository.is_private',
    'currentRepository.project.owner.uuid',
    'currentRepository.project.workspace.uuid',
]
