#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 30 10:24:12 2020

@author: frank
"""
from flask import Flask
app = Flask(__name__)

#@app.route('/user/<int:user_id>')
@app.route('/')
@app.route('/hello')
@app.route('/hello/<name>')

#def get_user(user_id):
#    return 'User ID: %d' % user_id

def hello(name=None):
    if name is None:
        name = 'World'
    return 'Hello %s' % name



if __name__ == '__main__':
    app.run()

