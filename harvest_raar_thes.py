#!/usr/bin/env python3

##
## harvest_raar_thes.py - harvest_raar_thes
##
## copyright (c) 2011-2012 Koninklijke Bibliotheek - National library of the Netherlands.
##
## this program is free software: you can redistribute it and/or modify
## it under the terms of the gnu general public license as published by
## the free software foundation, either version 3 of the license, or
## (at your option) any later version.
##
## this program is distributed in the hope that it will be useful,
## but without any warranty; without even the implied warranty of
## merchantability or fitness for a particular purpose. see the
## gnu general public license for more details.
##
## you should have received a copy of the gnu general public license
## along with this program. if not, see <http://www.gnu.org/licenses/>.
##

import gzip,os,string,sys,time,random,unicodedata
import time
import urllib.request, urllib.parse, ast
import http.client, re

from pprint import pprint
from xml.etree import ElementTree as etree

import urllib

__author__ = "Willem Jan Faber"

change = [
{"a" : "d"}, 
{"i" : "l"}, 
{"e" : "c"}, 
{"nn" : "m"}, 
{"u" : "v"},
{"vv" : "w"},
{"h" : "n"},
{"q" : "g"}
]

def generate_words(word):
    print(word)
    for item in change:
        if word.find(list(item.keys())[0]) > -1:
            word=word.replace(list(item.keys())[0], list(item.values())[0])
    print(word)
    return(word)


def post_url(data):
    headers = {"Content-type" : "text/xml; charset=utf-8", "Accept": "text/plain"}
    conn = http.client.HTTPConnection("localhost:8080")
    try:
        conn.request("POST","/solr/ggc-thes-raar/update/", bytes(data.encode('utf-8')), headers)
        response = conn.getresponse()
        res = response.read()
        if not str(res).find("<int name=\"status\">0</int>") > -1:
            print(res)
        conn.close()
    except:
        pass
    return()

class OaiHarvest(object):
    baseurl = ""
    unwanted_tags = "responseDate","request","datestamp"

    def __init__(self, baseurl) :
        self.baseurl = baseurl

    def _get(self, url, xml_parse=False):
        return(urllib.request.urlopen(str(url)).read().decode("utf-8"))

    def listIdentifiers(self, setname, token):
        identifier=[]
        resumptiontoken=False

        if (type(token) != type(True)):
            url=self.baseurl+"?verb=ListIdentifiers&set=GGC-THES&metadataPrefix=dcx&resumptionToken="+token
        else:
            url=self.baseurl+"?verb=ListIdentifiers&set=GGC-THES&metadataPrefix=dcx"
        try:
            record=self._get(url)
        except:
            os._exit(-1)

        for line in record.split("<"):
            if line.find(">") > -1:
                if line.find("status>deleted") > -1:
                    identifier.pop()
                if (line.find(":") > -1 or line.find("!!") > -1):
                    if line.lower().startswith("identifier>"):
                        identifier.append(line.split(">")[1].strip())

        for line in record.split("<"):
            if line.find(">") > -1:
                if line.lower().startswith("resumptiontoken>"):
                    resumptiontoken=line.split(">")[1].strip()

        return(resumptiontoken, identifier)

    def getRecord(self, identifier):
        data=[]
        count=0
        record=etree.XML(self._get(self.baseurl+"?verb=GetRecord&identifier="+identifier, True))

        skip = False4
        doc=etree.Element("doc")

        add=etree.SubElement(doc, 'field', {"name" : "id"})
        add.text=identifier

        add=etree.SubElement(doc, 'field', {"name" : "id_str"})
        add.text=identifier

        id=":".join(identifier.split(":")[1:])
        for item in record.getiterator():
            if item.tag.split("}")[1]:
                name=""
                if len(item.attrib) == 1:
                    name = item.tag.split("}")[1]
                    if not item.text:
                        for key in item.attrib.keys():
                            value=item.attrib[key]
                    elif len(item.text.strip()) == 0:
                        for key in item.attrib.keys():
                            value=item.attrib[key]
                    else:
                        value=item.text
                else:
                    if not item.tag.endswith("RDF"):
                        name=item.tag.split("}")[1]
                        value=item.text
                print(name)
                if len(name) > 0:
                    if name == "prefLabel" or name == "altLabel":
                        hask = value.split("(")[0].strip()
                        print(hask,"!!!")

                        add=etree.SubElement(doc, 'field', {"name" : name})
                        add.text=hask

                        add=etree.SubElement(doc, 'field', {"name" : name+"_str"})
                        add.text=hask
                        
                        word = generate_words(hask)

                        add=etree.SubElement(doc, 'field', {"name" : name})
                        add.text=word

                        add=etree.SubElement(doc, 'field', {"name" : name+"_str"})
                        add.text=word

                        if value.find(")") > -1:
                            add=etree.SubElement(doc, 'field', {"name" : name+"_full"})
                            add.text=value

                            add=etree.SubElement(doc, 'field', {"name" : name+"_full_str"})
                            add.text=value
 
                            res=re.match(".+?(\d{4}).+(\d{4})+", value)
                            if not res == None:

                                add=etree.SubElement(doc, 'field', {"name" : "start_date"})
                                add.text=res.group(1)

                                add=etree.SubElement(doc, 'field', {"name" : "end_date"})
                                add.text=res.group(2)
                            else:
                                res=re.match(".+?(\d{4}).+", value)
                                if not res == None:
                                    add=etree.SubElement(doc, 'field', {"name" : "start_date"})
                                    add.text=res.group(1)

                else:
                    add=etree.SubElement(doc, 'field', {"name" : name})
                    add.text=value

                    add=etree.SubElement(doc, 'field', {"name" : name+"_str"})
                    add.text=value.split("(")[0]

                if item.tag.endswith("metadata"):
                    skip = False
        return(etree.tostring(doc))


OAI_BASEURL="http://services.kb.nl/mdo/oai"
OAI_DEV_BASEURL="http://serviceso.kb.nl/mdo/oai"

if __name__ == "__main__":
    oaiharvester = OaiHarvest(OAI_DEV_BASEURL)
    token="GGC-THES!!!dcx!100"

    while(token):
        fail=False
        last_token=token
        (token, identifiers) = oaiharvester.listIdentifiers("GGC-THES", token)
        add=""
        print("Working on token : " +str(token))
        add+="<add>"
        if len(identifiers) > 0:
            for identifier in identifiers:
                try:
                    add+=oaiharvester.getRecord(identifier)
                except:
                    pass
        add+="</add>"
        print(add)
        os._exit(-1)
