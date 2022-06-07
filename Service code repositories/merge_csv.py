#*******************************************************************************
# Copyright (C) 2022 AIR Institute
# 
# This program and the accompanying materials are made
# available under the terms of the Eclipse Public License 2.0
# which is available at https://www.eclipse.org/legal/epl-2.0/
# 
# SPDX-License-Identifier: EPL-2.0
# 
# Contributors:
#    Adrian Diarte Prieto - initial API and implementation
#*******************************************************************************

import os
import glob
import pandas as pd
from pathlib import Path

#set working directory
os.chdir(Path(__file__).parent)

extension = 'csv'
filenames = [i for i in glob.glob('*.{}'.format(extension))]

csv = pd.concat([pd.read_csv(f, index_col=[0]) for f in filenames ])
csv = csv.reset_index(drop=True)
csv.to_csv("_merged.csv", header=True, encoding='utf-8-sig')
