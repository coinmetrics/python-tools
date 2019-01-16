import requests
import json
import base64
import time


class RpcCallFailedException(Exception):
    pass


class JsonRpcCaller(object):

    def __init__(self, host, port, user, password, queryPath="", tls=False, tlsVerify=True):
        self.host = host
        self.port = str(port)
        self.user = user
        self.password = password
        self.queryPath = queryPath
        self.tls = tls
        self.tlsVerify = tlsVerify

    def getAddress(self):
        return self.host + ":" + self.port

    def makeRpcCall(self, headers, payload):
        try:
            protocol = "https" if self.tls else "http"
            response = requests.post("{0}://{1}:{2}/{3}".format(protocol, self.host, self.port, self.queryPath),
                                     headers=headers, data=payload, verify=(self.tls and self.tlsVerify))
        except Exception as e:
            raise RpcCallFailedException(e)

        if response.status_code != 200:
            raise RpcCallFailedException("Invalid status code: %s" % response.status_code)

        responseJson = response.json(parse_float=lambda f: f)
        if type(responseJson) != list:
            if "error" in responseJson and responseJson["error"] is not None:
                raise RpcCallFailedException("RPC call error: %s" % responseJson["error"])
            else:
                return responseJson["result"]
        else:
            result = []
            for subResult in responseJson:
                if "error" in subResult and subResult["error"] is not None:
                    raise RpcCallFailedException("RPC call error: %s" % subResult["error"])
                else:
                    result.append(subResult["result"])
            return result

    def call(self, method, params=None):
        if params is None:
            params = []
        authString = str(base64.b64encode("{0}:{1}".format(self.user, self.password).encode()))[2:-1]
        headers = {'content-type': 'application/json', 'Authorization': 'Basic ' + (authString)}
        payload = json.dumps({"jsonrpc": "2.0", "id": "0", "method": method, "params": params})
        return self.makeRpcCall(headers, payload)

    def bulkCall(self, methodParamsTuples):
        authString = str(base64.b64encode("{0}:{1}".format(self.user, self.password).encode()))[2:-1]
        headers = {'content-type': 'application/json', 'Authorization': 'Basic ' + (authString)}
        payload = json.dumps([{"jsonrpc": "2.0", "id": "0", "method": method, "params": params}
                              for method, params in methodParamsTuples])
        return self.makeRpcCall(headers, payload)
