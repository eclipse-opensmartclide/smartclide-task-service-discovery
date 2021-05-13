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
csv.to_csv("GitHub_Top1k_keywords.csv", header=True, encoding='utf-8-sig')