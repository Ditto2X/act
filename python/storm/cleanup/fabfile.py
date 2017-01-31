# Fabric file to install and configure the storm cleanup process.

from fabric.api import *
from string import Template
import tempfile

env.hosts = ['host1', 'host2']

mongoservers = [
    'mongoserver1:27017',
    'mongoserver2:27017',
    'mongoserver3:27017',
]

databases = [
    'db1',
    'db2'
]
config = """---
mongoservers:
$mongoservers

databases:
$databases

searchdir: $cleandir
"""

def yamllist(items) :
    """
    Turns an array into one suitable for yaml.
    """
    return "\n".join([ "- %s" % x for x in items])

def setupdir(values) :
    source = Template(config)

    with settings(warn_only=True) :
        if run("test -d ~/STORM_CLEANUP").failed :
            run("mkdir STORM_CLEANUP")

        with cd('STORM_CLEANUP') :
            put('cleanup.py', 'cleanup.py', mode=0755)
            with tempfile.NamedTemporaryFile() as f :
                f.write(source.substitute(values))
                f.flush()
                put(f.file, 'cleanup.yaml')

def getstormdir() :
    with settings(warn_only=True) :
        if run("test -d /data1/stormexports").succeeded :
            return "/data1/stormexports"

        if run("test -f /apps/storm/conf/server.properties").failed :
            return '/data1/tmp'

        with cd('/apps/storm/conf') :
            stormdir = run("grep -E 'prop.temp.file.path\s*=\s*' server.properties | sed -e 's/.*=\s*//'")
            return stormdir

def cleanup() :
    with settings(warn_only=True) :
        if run("test -d ~/STORM_CLEANUP").succeeded :
            run("rm -rf ~/STORM_CLEANUP")

def install() :
    values =  {
        'mongoservers': yamllist(mongoservers),
        'databases': yamllist(databases),
        'cleandir': getstormdir()
    }

    setupdir(values)
