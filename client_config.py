#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
@date: 2020-05-13
@author: wangzg
@license: MIT
'''
from __future__ import absolute_import, division, \
    print_function, unicode_literals
import sys
import requests

BASEURL = 'http://localhost:6666'


def get_measurements():
    resp = requests.get(BASEURL + '/config/measurements')
    print(resp.status_code, resp.content)
    return resp.status_code, resp.content


def get_backends():
    resp = requests.get(BASEURL + '/config/backends')
    print(resp.status_code, resp.content)
    return resp.status_code, resp.content


def update_measurements():
    resp = requests.post(BASEURL + '/config/measurements', json={'cpu1': ['local', 'local2']})
    print(resp.status_code, resp.content)
    return resp.status_code, resp.content


def update_backends():
    resp = requests.post(BASEURL + '/config/backends', json={
        "local4": {
            "url": "http://localhost:8087",
            "db": "test",
            "zone": "local",
            "interval": 1000,
            "timeout": 10000,
            "querytimeout": 600000,
            "maxrowlimit": 10000,
            "checkinterval": 1000,
            "rewriteinterval": 10000,
            "writeonly": 0
        }
    })
    print(resp.status_code, resp.content)
    return resp.status_code, resp.content


def del_measurement():
    resp = requests.delete(BASEURL + '/config/measurements/' + 'cpu1')
    print(resp.status_code, resp.content)
    return resp.status_code, resp.content


def del_backend():
    resp = requests.delete(BASEURL + '/config/backends/' + 'local4')
    print(resp.status_code, resp.content)
    return resp.status_code, resp.content


def main():
    assert get_measurements()[0] == 200
    assert get_backends()[0] == 200

    assert update_backends()[0] == 200
    assert update_measurements()[0] == 200

    assert del_backend()[0] == 200
    assert del_measurement()[0] == 200


if __name__ == '__main__':
    main()
