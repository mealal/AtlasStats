# The script reqires installation of requests module
# pip install requests

import argparse
from datetime import datetime
import time
import requests
from requests.auth import HTTPDigestAuth

atlas_uri = "https://cloud.mongodb.com/api/atlas/v1.0/"

# API keys may be provided as command line parameters or using the following variables
publickey = "#"
privatekey = "#"

class Util:
    def logit(self, message, log_type = "INFO"):
        cur_date = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        stamp = f"{cur_date}|{log_type}> "
        if type(message) == dict or type(message) == list:
            message = str(message)
        for line in message.splitlines():
            print(f"{stamp}{line}")

    def message_box(self, msg, mtype = "sep"):
        tot = 100
        start = ""
        res = ""
        msg = msg[0:84] if len(msg) > 85 else msg
        ilen = tot - len(msg)
        if (mtype == "sep"):
            start = f'#{"-" * int(ilen/2)} {msg}'
            res = f'{start} {"-" * (tot - len(start) + 1)}#'
        else:
            res = f'#{"-" * tot}#\n'
            start = f'#{" " * int(ilen/2)} {msg} '
            res += f'{start}{" " * (tot - len(start) + 1)}#\n'
            res += f'#{"-" * tot}#\n'
        self.logit(res)
        return res

    def report_header(self, filename, content):
        with open(filename, 'w') as lgr:
            lgr.write(f'{content}\n')

    def report_line(self, filename, content):
        with open(filename, 'a') as lgr:
            lgr.write(f'{content}\n')
def init():
   parser = argparse.ArgumentParser(description="MongoStream")
   parser.add_argument('-p', '--public_key', type=str, help="Public key", required=False, default=publickey)
   parser.add_argument('-s', '--private_key', type=str, help="Private key", required=False, default=privatekey)
   parser.add_argument('-f', '--file', type=str, help="Report file", required=False, default="report.csv")
   args = parser.parse_args()

   return args

def getAPI(publicKey, privateKey, endpoint="", itemsPerPage=500, pageNum=1, params={}):
    uri = atlas_uri+endpoint+f"?itemsPerPage={itemsPerPage}&pageNum={pageNum}"
    for p in params:
        v = params[p]
        if type(v) is list:
            for pp in v:
                param = f"&{p}={pp}"
        else:
            param = f"&{p}={v}"
        uri += param
    response=requests.get(uri, auth=HTTPDigestAuth(publicKey, privateKey))
    if (response.status_code != 200):
        bb.logit(f"{response.reason}", "ERROR")
        bb.logit(f"{response.json()['detail']}", "ERROR")
        raise Exception(f"GET API call error: {response.reason}")
    return response.json()

def getClusters(publicKey, privateKey):
    clusters = []

    res = getAPI(publicKey, privateKey, "clusters")
    for val in res['results']:
        for cl in val['clusters']:
            cluster = {
                'orgId': val['orgId'],
                'orgName': val['orgName'],
                'projectId': val['groupId'],
                'projectName': val['groupName'],
                'clusterId': cl['clusterId'],
                'clusterName': cl['name'],
                'nodeCount': cl['nodeCount'],
                'type': cl['type'],
                'version': cl['versions'][0],
                'dataSizeBytes': cl['dataSizeBytes']
            }
            clusters.append(cluster)
    return clusters

def getPrimaryNode(publicKey, privateKey, projectId):
    node = {}
    res = getAPI(publicKey, privateKey, f"groups/{projectId}/processes")
    for val in res['results']:
        if (val['typeName'] == 'REPLICA_PRIMARY'):
            node = {
                'id': val['id'],
                'hostname': val['hostname'],
                'port': val['port'],
                'projectId': projectId
            }
    return node

def getDatabases(publicKey, privateKey, projectId, hostId, rcount):
    databases = []
    pageNum = 1
    cnt = rcount
    dbcnt = 0

    while (pageNum == 1 or dbcnt > 0):
        if (cnt >= 99):
            cnt = 0
            sleep()
        res = getAPI(publicKey, privateKey, f"groups/{projectId}/processes/{hostId}/databases", pageNum=pageNum)
        dbcnt = 0
        pageNum += 1
        for val in res['results']:
            dbcnt += 1
            databases.append({'name':val['databaseName']})

    return databases,cnt

def getDatabaseStats(publicKey, privateKey, projectId, hostId, dbname):
    stats = {
        'databaseDataSize': 0,
        'databaseStorageSize': 0,
        'databaseIndexSize': 0
        }

    res = getAPI(publicKey, privateKey, f"groups/{projectId}/processes/{hostId}/databases/{dbname}/measurements", params={"granularity": "PT1M", "period": "PT1H"})
    for val in res['measurements']:
        value = 0
        l = len(val['dataPoints'])
        if (l>0):
            value = val['dataPoints'][l-1]['value']
        if (val['name'] == "DATABASE_DATA_SIZE"):
            stats['databaseDataSize'] = value
        elif (val['name'] == "DATABASE_STORAGE_SIZE"):
            stats['databaseStorageSize'] = value
        elif (val['name'] == "DATABASE_INDEX_SIZE"):
            stats['databaseIndexSize'] = value

    return stats

def sleep():
    bb.logit('Sleep to avoid api limitation')
    time.sleep(60)
    bb.logit('Resuming the process')

def run(settings):
    reportfile = settings.file
    publicKey = settings.public_key
    privateKey = settings.private_key

    bb.message_box(f"Report generation started to the file {reportfile}")
    
    bb.logit('Receiving list of clusters')
    clusters = getClusters(publicKey, privateKey)
    cnt = 1
    bb.logit(f'{len(clusters)} cluster(s) received')

    bb.logit('Receiving primary nodes')
    total = 0
    for i in range(len(clusters)):
        if (cnt >= 99):
            cnt = 0
            sleep()
        clusters[i]['primaryNode'] = getPrimaryNode(publicKey, privateKey, clusters[i]['projectId'])
        cnt += 1
        total += 1
    bb.logit(f'{total} node(s) received')

    bb.logit('Receiving list of databases')
    total = 0
    for i in range(len(clusters)):
        clusters[i]['databases'], cnt = getDatabases(publicKey, privateKey, clusters[i]['projectId'], clusters[i]['primaryNode']['id'], cnt)
        total += len(clusters[i]['databases'])
    bb.logit(f'{total} database(s) received')

    bb.logit('Receiving databases stats')
    total = 0
    for i in range(len(clusters)):
        for d in range(len(clusters[i]['databases'])):
            if (cnt >= 99):
                cnt = 0
                sleep()
            clusters[i]['databases'][d]['stats'] = getDatabaseStats(publicKey, privateKey, clusters[i]['projectId'], clusters[i]['primaryNode']['id'], clusters[i]['databases'][d]['name'])
            cnt += 1
            total += 1
    bb.logit(f'{total} database(s) stats received')

    bb.message_box("Saving results to the file")
    bb.report_header(reportfile, "OrgID,OrgName,ProjectId,ProjectName,ClusterId,ClusterName,ClusterType,ClusterNodeCount,ClusterDataSizeBytes,Hostname,DBVersion,DatabaseName,DatabaseDataSize,DatabaseIndexSize,DatabaseStorageSize")
    for i in range(len(clusters)):
        cluster = clusters[i]
        for d in range(len(clusters[i]['databases'])):
            db = cluster['databases'][d]
            bb.report_line(reportfile, f"{cluster['orgId']},{cluster['orgName']},{cluster['projectId']},{cluster['projectName']},{cluster['clusterId']},{cluster['clusterName']},{cluster['type']},{cluster['nodeCount']},{cluster['dataSizeBytes']},{cluster['primaryNode']['hostname']},{cluster['version']},{db['name']},{db['stats']['databaseDataSize']},{db['stats']['databaseIndexSize']},{db['stats']['databaseStorageSize']}")

    bb.message_box("Report generation ended")

if __name__ == "__main__":
   global bb
   bb = Util()

   settings = init()
   run(settings)
