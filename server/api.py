#!/usr/bin/python
# -*- coding: utf-8 -*- 

from flask import Flask, Response, jsonify, request
import redis
import json
import sys

class RedisJson() :
 def __init__(self, host='localhost', port=6379) :
  self.__rcon = ''
  self.data = ''
  self.__connect()

 def __connect(self, host='localhost', port=6379) :
  if self.__rcon == '' :
   self.__rcon = redis.Redis(host=host,port=port,db=0)

 def __make_jsonobjstr(self,k, v) :
  r = '{"' + k + '":' + '"' + v + '"}'
  #print r
  return r

 def SetupJson(self) :
  self.__connect()

  keys = self.__rcon.keys()

  jsonstring = '{"records":['

  for k in keys :
   v = self.__rcon.get(k)
   tmp = self.__make_jsonobjstr(k,v) + ','
   jsonstring += tmp
  
  jsonstring = jsonstring[:-1]
  jsonstring += ']}'
  print jsonstring

  self.data = json.loads(jsonstring)
  return self.data

 def SetupJsonp(self, callback='function' ) :
  self.SetupJson()
  return Response(
   "%s(%s);" %(callback, json.dumps(self.data)),
   mimetype="text/javascript")



api = Flask(__name__)

@api.route('/getJson', methods=['POST'])
def get_json() :
 r = RedisJson()
 return jsonify(r.SetupJson())

@api.route('/getJsonp', methods=['GET'])
def get_jsonp() :
 r = RedisJson()
 callback = request.args.get("callback")
 return r.SetupJsonp(callback)

if __name__ == '__main__' :
 args = sys.argv
 host = args[1]
 port = args[2]
 api.run(host=host, port=port)
