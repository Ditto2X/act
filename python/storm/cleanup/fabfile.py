# Fabric file to install and configure the storm cleanup process.

from fabric.api import *
from string import Template
import tempfile

env.hosts = [
    'host001',
    'host002'
]

mongoservers = [
    'mongoserver001:27017',
    'mongoserver002:27017',
    'mongoserver003:27017'
]

databases = [
    'db001',
    'db002',
    'db003'
]

config = """---
mongoservers:
$mongoservers

databases:
$databases

searchdir:
$cleandir
"""

cron = """
# Cleanup storm data directory. - Author - 20170131
# $hour * * * * $path/cleanup.py --purge
"""

def yamllist(items) :
    """
    Turns an array into one suitable for yaml.
    """
    return "\n".join([ "- %s" % x for x in items])

def setupdir(values) :
    """
    Creates the directory if needed, and copies files into place.
    """
    source = Template(config)
    crontab = Template(cron)

    with settings(warn_only=True) :
        if run("test -d $path/STORM_CLEANUP").failed :
            sudo("umask 022 ; mkdir -p $path/STORM_CLEANUP && sudo chown sysops:sysops $path/STORM_CLEANUP")

        with cd('$path/STORM_CLEANUP') :
            put('cleanup.py', 'cleanup.py', mode=0755)
            with tempfile.NamedTemporaryFile() as f :
                f.write(source.substitute(values))
                f.flush()
                put(f.file, 'cleanup.yaml')
            with tempfile.NamedTemporaryFile() as f :
                f.write(crontab.substitute(values))
                f.flush()
                put(f.file, 'crontab.txt')
            run('crontab crontab.txt')

def getstormdir() :
    """
    Checks with the storm configuration to get location of output files.
    """
    with settings(warn_only=True) :
        if run("test -d /data1/stormexports").succeeded :
            return "/data1/stormexports"

        if run("test -f /apps/storm/conf/server.properties").failed :
            return '/data1/tmp'

        with cd('/apps/storm/conf') :
            stormdir = run("grep -E 'prop.temp.file.path\s*=\s*' server.properties | sed -e 's/.*=\s*//'")
            return stormdir

def cleanup() :
    """
    Removes the directory setup by this script.
    """
    with settings(warn_only=True) :
        if run("test -d ~/STORM_CLEANUP").succeeded :
            run("rm -rf ~/STORM_CLEANUP")

def install() :
    """
    Makes it all happen.
    """

    values =  {
        'mongoservers': yamllist(mongoservers),
        'databases': yamllist(databases),
        'cleandir': yamllist([getstormdir()]),
        'hour': (env.hosts.index(env.host) % 6) * 10
    }

    setupdir(values)
