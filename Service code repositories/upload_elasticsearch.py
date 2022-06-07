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

import csv
from os import scandir
from datetime import datetime
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection

# Initial vars
route = "./test/"
url = ""

# Create the elasticsearch client.
es = Elasticsearch(
    hosts = url,
    timeout = 30,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    http_auth=('', '')
    )

# Search all files in route
files= [obj.name for obj in scandir(route) if obj.is_file()]
for file in files:
    # Open and upload the file
    with open(route+file, encoding="utf-8") as f:
        print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tEmpezando "+str(file))
        reader = csv.DictReader(f)
        helpers.bulk(es, reader, index='pruebagitlab')
        print("["+str(datetime.now().strftime('%H:%M:%S'))+"] \tAcabado "+str(file))
