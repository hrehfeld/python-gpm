import os
import requests
import argparse
from pathlib import PurePath, Path
import json

import semver
import hashlib
from collections import OrderedDict as odict

from zipfile import ZipFile
import shutil

import datetime

import time

date_format = '%Y-%m-%d %H:%M:%S %z'

default_type_data = {'quake-bsp': {}}

repo_ext = '.json'
repo_files_ext = '.files' + repo_ext
state_ext = '.json'

repo_delim = '\\'

config_dir = Path(os.path.expanduser('~')) / Path('.python-gpm')
state_dir = config_dir / 'state'
installed_state_dir = state_dir / 'installed'
installed_state_file = (state_dir / 'installed').with_suffix(state_ext)

def write_installed_state(state):
    with installed_state_file.open('w') as f:
        json.dump(state, f)

def load_installed_state():
    if installed_state_file.exists():
        return load_json(installed_state_file)
    return odict()

def package_state_path(name):
    p = (installed_state_dir / name)
    return p.with_suffix(p.suffix + state_ext)

def package_backup_file(name):
    return package_state_path(name).with_suffix('.package')

quaddicted_local = ('remote/quaddicted.json', 'remote/quaddicted.files.json')
repositories = {'quaddicted': [('file://' + p for p in quaddicted_local)]}

repos_filepath = Path('repos')

repo_dl_dir = "https://www.quaddicted.com/filebase/"
repo_dl_dir = "file:///home/hrehfeld/projects/quakeinjector/download/"

def repo_download_url(package, path):
    return repo_dl_dir + path

file_prot = 'file://'
def is_url(url):
    #todo better support file://
    return not url.startswith(file_prot)

def uri2path(uri):
    return Path(uri[len(file_prot):])
    
    
def get_uri(url, binary=True):
    if is_url(url):
        print('Downloading %s...' % url)
        r = requests.get(url)
        
        if r.status_code != 200:
            raise FileNotFoundError('Could not download file: %i (%s)' % (r.status_code, url))
        return r.content if binary else r.text
    else:
        with uri2path(url).open('rb' if binary else 'r') as fd:
            return fd.read()
                

cache_dir = Path('/tmp/ppm/')
backup_dir = cache_dir / '.backup'

def cache_path(package, path=None):
    r = cache_dir / (package['name'] + '-' + package['version'])
    if not path:
        return r
    return r / path

def cache_current(package, path):
    return cache_path(package, path).exists()

package_keys = ['type', 'name', 'version', 'description', 'keywords', 'author', 'contributors', 'bugs', 'homepage', 'dependencies']

hash_key = 'sha1'




def hash_path(path):
    with path.open('rb') as f:
        return hash_file(f)

def hash_file(f):
    hash = hashlib.sha1()
    #todo read chunks
    hash.update(f.read())
    return hash.hexdigest()

def is_dir(pinfo):
    return pinfo['size'] == 0

def is_container(f):
    #todo support more
    return f.suffix == '.zip'

def container_fileinfos(path):
    files = odict()
    with ZipFile(str(path), 'r') as zf:
        l = zf.filelist
        for subpath in l:
            r = odict()
            r['size'] = subpath.file_size
            #directories excluded
            if subpath.file_size > 0:
                with zf.open(subpath.filename, 'r') as f:
                    hash = hash_file(f);
                r[hash_key] = hash


            files[subpath.filename] = r
#    print(files)
    return files
            
                
    

class DefaultHandler:
    ext = 'zip'

    def default_files(self, name):
        p = Path(name)
        return [p.with_suffix(p.suffix + '.' + self.ext)]
        

class QuakeBsp(DefaultHandler):
    name = 'quake-bsp'

    quake_path = Path('/home/hrehfeld/projects/quake/')
    package_keys = ['title', 'zipbasedir', 'commandline', 'startmap', 'date']
    

    def basedir(self, subp):
        return subp.get('zipbasedir', 'id1')

    def get_install_path(self, basedir, path):
        return self.quake_path / basedir / path
    
    def add(self, names_versions, repo_data, repo_files):
        for (name, version) in names_versions:
            qdata = repo_data[name][version]['type'][self.name]
            
            for k in list(qdata.keys()):
                if k not in self.package_keys:
                    warn('Illegal key %s.type.%s.%s' % (data['name'], self.name, k))
                    del (qdata[k])
                    

    def install(self, p, files, cachep, force_write):
        name = p['name']

        subp = p['type'][self.name]
        basedir = self.basedir(subp)

        qpath = self.quake_path
        #todo handle
        assert(qpath.exists())

        def copy(force_write, dryrun, dlfd, path, f):
            installp = self.get_install_path(basedir, path)
            relp = PurePath(basedir) / path
            def write():
                if not dry_run:
                    installp.parent.mkdir(parents=True, exist_ok=True)
                    if isdir:
                        #create dirs as well
                        log('creating dir %s' % installp)
                        installp.mkdir()
                    else:
                        #write file
                        with installp.open('wb') as fd:
                            log('writing %s' % installp)
                            shutil.copyfileobj(dlfd, fd)
                return path
            
            def backup():
                bp = backup_dir / relp
                bp.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(installp), str(bp))
                
            isdir = is_dir(f)
            exists = installp.exists()
            #check if f exists
            if exists:
                if isdir:
                    if installp.is_dir():
                        return None
                    else:
                        if force_write:
                            backup()
                        else:
                            return 'directory %s exists' % installp
                else:
                    if installp.is_dir():
                        if force_write:
                            backup()
                        else:
                            return 'directory %s exists, expected file' % installp
                    else:
                        # and compare hash
                        ha = hash_path(installp)
                        if ha == f[hash_key]:
                            return None
                        else:
                            if force_write:
                                backup()
                            else:
                                return 'file %s exists with different hash' % installp
            return write()
        written_files = []
        errors = []
        dry_run = False
        def add(r):
            if isinstance(r, str):
                errors.append(r)
                dry_run = True
            elif r is None:
                pass
            else:
                written_files.append(str(r) )
        for path, f in files.items():
            if f['subfiles']:
                cpath = (cachep / path)
                #print('opening %s' % cpath)
                with ZipFile(str(cpath), 'r') as zf:
                    for subpath, subf in f['subfiles'].items():
                        add(copy(force_write, dry_run, zf.open(subpath, 'r'), Path(subpath), subf))
            else:
                add(copy(force_write, dry_run, Path(path), f))
        
        #todo handle errors
        #print('errots: %s' % errors)
        return {'written': written_files}

    def remove(self, pkg, state):
        written_files = state['written']
        #print(pkg)
        files = pkg['files']

        subpkg = pkg['type'][self.name]
        basedir = self.basedir(subpkg)

        to_remove = []
        install_paths = [self.get_install_path(basedir, path) for path in written_files]

        for path, install_path in zip(written_files, install_paths):
            #print(path)
            if not install_path.exists():
                raise Exception('%s was written, but does not exist anymore')
            
            pinfo = None
            if path in files:
                pinfo = files[path]
            else:
                #check container files
                for fp, f in files.items():
                    if 'subfiles' in f:
                        sfiles = f['subfiles']
                        #todo handle relative paths?
                        if path in sfiles:
                            pinfo = sfiles[path]
                            break
            if not pinfo:
                raise Exception('%s was written, but was not found in pkg file for  %s' % (path, pkg['name']))
            if is_dir(pinfo):
                #for subf in install_path.iterdir():
                #    if subf not in install_paths:
                #        raise Exception('File %s in %s/ was not installed, cannot delete dir' % (subf, path))
                if not len(installed_path.iterdir()):
                    to_remove.append(install_path)
            else:
                stats = install_path.stat()
                if pinfo['size'] != stats.st_size:
                    raise Exception('%s was written, but was modified' % (path))
                if pinfo[hash_key] != hash_path(install_path):
                    raise Exception('%s was written, but was modified' % (path))
                to_remove.append(install_path)
        #print('removing %s.' % ', '.join([str(p) for p in to_remove]))

        dirs = []
        for path in to_remove :
            if path.is_dir():
                dirs.append(path)
                continue
            log('Removing %s' % path)
            path.unlink()

        for path in dirs:
            log('Removing %s' % path)
            path.rmdir()


handlers = { QuakeBsp.name: QuakeBsp }

def repo_filepath(repo):
    return (repos_filepath / repo).with_suffix(repo_ext)

def repo_files_path(repo):
    return repo_filepath(repo).with_suffix(repo_files_ext)

def subrepo_path(repo, handler):
    p = (repos_filepath / (repo + '-' + handler.name))
    return p.with_suffix(p.suffix + repo_ext)


def log(msg):
    print(msg)

def warn(msg):
    print('WARNING:', msg)

class Package:
    def __init__(self, **kwargs):
        for a in package_keys:
            setattr(self, a, kwargs.get('name', None))

def add_s(force, paths, packages, package_data, repo_data, repo_files):
    handler_data = odict()
    for path, data in zip(paths, packages):
#        p = Package(**data)
        name = data['name']
        log('adding %s...' % name)

        version = data['version']

        if name in repo_data:
            if not force and not semver.compare(version, repo_data[name]['version']) > 0:
                raise Exception('Package exists and version is not higher than existing package')
            log('updating package ' + name)
        elif name in package_data:
            raise Exception('Package exists in other repo')

        type_data = data.get('type', default_type_data)
        types = type_data.keys()
        for t in types:
            assert(t in handlers)
        _handlers = [handlers[t]() for t in types]

        files = data.get('files', [])
        if files:
            del data['files']
            files = [Path(f) for f in files]
        if not files:
            for handler in _handlers:
                f = handler.default_files(name)
                files += f
        real_files = [path / f for f in files]
        for f in real_files:
            if not f.exists():
                raise FileNotFoundError(str(f))
        file_infos = odict()
        for f, real_f in zip(files, real_files):
            r = odict()
            r[hash_key] = hash_path(real_f)
            r['size'] = real_f.stat().st_size
            if is_container(real_f):
                r['subfiles'] = container_fileinfos(real_f)
            file_infos[str(f)] = r
            

        for t in types:
            handler_data.setdefault(t, [])
            handler_data[t].append((name, version))

        p = odict()
        for k in package_keys:
            if k in data:
                p[k] = data[k]
        repo_data.setdefault(name, odict())
        repo_data[name][version] = p
        repo_files.setdefault(name, odict())
        repo_files[name][version] = file_infos

    #sort by version
    for name, versions in repo_data.items():
        repo_data[name] = odict(sorted(versions.items(), key=lambda t: t[0]))

    for name, versions in repo_files.items():
        repo_files[name] = odict(sorted(versions.items(), key=lambda t: t[0]))
        
    for t, names in handler_data.items():
        handler = handlers[t]()
        #modifies data
        handler.add(names, repo_data, repo_files)

def add(args, package_data, repos, files):
    repo = args.repo
    repo_data = []
    repo_files = odict()
    if repo in repos:
        repo_data = repos[repo]
        repo_files = repos[repo]

    paths = args.paths

    for path in paths:
        pjson = path / 'package.json'

        with pjson.open('r') as f:
            data = json.load(f)

            add_s(args.force, [path], [data], package_data, repo_data, repo_files)

    repo_path = repo_filepath(repo)
    log('Writing ' + str(repo_path))
    write_repo(repo_path, list(repo_data.values()))
    
def make_dirs(path):
    created = []
    for p in reversed([path] + list(path.parents)):
        #print(p)
        if not p.exists():
            p.mkdir()
            created.append(p)
    return created
    
def compare_installed_version(name, cmpversion):
    p = load_json(package_backup_file(name))
    version = p['version']
    return version, semver.compare(cmpversion, version)

def installed_version(name):
    p = load_json(package_backup_file(name))
    version = p['version']
    return version

def latest_version(versions):
    return sorted(versions)[-1]

def install(args, package_data, repo_data):
    make_dirs(installed_state_dir)

    installed_packages = load_installed_state()
    packages = args.packages
    to_install = []
    for name in packages:
        if name not in package_data:
            raise Exception('No such package: ' + name)
        version = latest_version(package_data[name].keys())
        p = package_data[name][version]

        if name in installed_packages:
            instversion = installed_version(name)
            if semver.compare(instversion, p['version']) != -1:
                raise Exception('Package %s already installed at version %s (trying: %s)' %  (name, instversion, p['version']))
        def match_version(v, matchv):
            # version Must match version exactly
            # >version Must be greater than version
            # >=version etc
            # <version
            # <=version
            # ~version "Approximately equivalent to version" See semver
            # ^version "Compatible with version" See semver
            # 1.2.x 1.2.0, 1.2.1, etc., but not 1.3.0
            # http://... See 'URLs as Dependencies' below
            # * Matches any version
            # "" (just an empty string) Same as *
            # version1 - version2 Same as >=version1 <=version2.
            # range1 || range2 Passes if either range1 or range2 are satisfied.
            # git... See 'Git URLs as Dependencies' below
            # user/repo See 'GitHub URLs' below
            # tag A specific version tagged and published as tag See npm-tag
            # path/path/path See Local Paths below
            m = matchv.strip()
#            mvs = m.split()
#            if len(vs) > 1:
#                return all([match_version(v, mv) for mv in mvs])
            # * Matches any version
            # "" (just an empty string) Same as *
            if m == '*' or m == '': return True
            c = semver.match
            # >version Must be greater than version
            # >=version etc
            # <version
            # <=version
            if m[:2] in ['>=', '<='] or m[0] in '><':
                return c(v, m)
            # parts = m.split('.')
            # for i, p in enumerate(parts):
            #     if p == 'x':
            # version Must match version exactly
            return c(v, '==' + m)

        #todo handle recursive deps
        for dep, req_version in p['dependencies'].items():
            if dep not in package_data:
                raise Exception('No such package: %s (as dependency of %s)' % (dep, name) )
            req_by = odict()
            if dep in installed_packages:
                dep_state = installed_packages[dep]
                dep_version = dep_state['version']
                req_by = dep_state['as_dependency']
                if match_version(dep_version, req_version):
                    req_by[name] = req_version
                    #matching version already installed
                    continue
            #need to update
            matching_version = False
            version_conflicts = odict()
            for avail_version, dep_pkg in package_data[dep].items():
                if match_version(avail_version, req_version):
                    #check if other packages require conflicting versions
                    cs = odict()
                    for oname, oversion in req_by.items():
                        if not match_version(avail_version, oversion):
                            cs[oname] = oversion
                    if cs:
                        version_conflicts[avail_version] = cs
                    else:
                        matching_version = avail_version
                        break
            if not matching_version:
                if version_conflicts:
                    msg = []
                    for v, cs in version_conflicts.items():
                        s = '%s: %s' % (v, ', '.join(['%s @%s' % o for o in cs.items()]))
                        msg.append(s)

                    raise Exception('Package %s required at version %s, but conflicting versions required by other packages: %s'
                                    %  (dep, req_version, '\n'.join(msg)))
                    
                raise Exception('Package %s required at version %s but no matching versions (found: %s)' %  (
                    dep, req_version, ', '.join(package_data[dep].keys())))
                        
            to_install.append((dep, matching_version, [(name, req_version)]))
        to_install.append((name, version, []))
    print('installing %s.' % to_install)
    for name, version, as_dependency in to_install:
        p = package_data[name][version]
        write_json(p, package_backup_file(name))

        cachedirp = cache_path(p)
        files = p['files']
        for path, f in files.items():
            cachep = cache_path(p, path)
            if cache_current(p, path):
                continue
            content = get_uri(repo_download_url(p, path))

            hash = hashlib.sha1()
            hash.update(content)
            ha = hash.hexdigest()
            if ha != f[hash_key]:
                raise Exception('Download for %s was broken (failed hash).' % url)

            cachep.parent.mkdir(parents=True, exist_ok=True)
            with cachep.open('wb') as f:
                f.write(content)

        print('installing', name)
        types = p['type']
        state = odict()
        for t in types:
            handler = handlers[t]()
            state[t] = handler.install(p, files, cachedirp, args.force)

        write_json(state, package_state_path(name))

        installed_packages[name] = odict([
            ('date', datetime.datetime.now(datetime.timezone.utc).strftime(date_format))
            , ('version', p['version'])
            , ('as_dependency', odict(as_dependency))
        ])
    write_installed_state(installed_packages)



def remove(args, package_data, repo_data):
    make_dirs(installed_state_dir)

    installed_packages = load_installed_state()
    packages = args.packages

    def is_required(*names):
        req = [[] for n in names]
        for o in installed_packages:
            p = package_data[o]
            #check if version in pkg list is still same
            ostate = installed_packages[o]
            if ostate['version'] != p['version']:
                #if not load version from state
                p = load_json(package_backup_file(o))

            for i, n in enumerate(names):
                if n in p['dependencies']:
                    req[i].append(o)
        return req

    def check_remove(name, also_removed=[], error_on_required=True, ):
        if name not in installed_packages:
            raise Exception('Package %s is not installed' % name)

        req_by = installed_packages[name]['as_dependency']
        req_by = [n for n in req_by.keys() if n not in also_removed]
        if req_by:
            if error_on_required:
                raise Exception('Package %s is still required by %s' % (name, ', '.join(req_by)))
            else:
                log('%s still required by %s' %(name, req_by))
                return []
            
        to_remove = [name]

        pkg = load_json(package_backup_file(name))
        deps = pkg['dependencies']
        for dep in deps:
            dep_req_by = installed_packages[dep]['as_dependency']
            if args.unneeded:
                #todo take care of circular references, esp if dep is req by other that is requiuired by another
                to_remove += check_remove(dep, also_removed + [name], False)
        return to_remove
        
                    
    to_remove = []
    for name in packages:
        to_remove += check_remove(name, packages)
    for name in to_remove:
        inst = installed_packages[name]
        
        state = load_json(package_state_path(name))

        version = latest_version(package_data[name].keys())
        pkg = package_data[name][version]
        #check if version in pkg list is still same
        if inst['version'] != pkg['version']:
            #if not load pkg from disk
            pkg = load_json(package_backup_file(name))

        types = pkg['type']
        for t in types:
            handler = handlers[t]()
            handler.remove(pkg, state[t])

        

        pkg_path = package_state_path(name)
        pkg_path.unlink()
        del installed_packages[name]
    write_installed_state(installed_packages)

def list_packages(args, package_data, repo_data):
    installed_packages = load_installed_state()
    preds = odict()
    formats = []
    if not args.all:
        preds['installed'] = lambda p: p in installed_packages
    else:
        formats.append(lambda p: p + ['[installed]'] if p[0] in installed_packages else p)
    if args.deps:
        preds['deps'] = lambda p: p in installed_packages and installed_packages[p]['as_dependency']
    if args.explicit:
        preds['explicit'] = lambda p: p in installed_packages and not installed_packages[p]['as_dependency']

    for p in package_data:
        if all([pr(p) for pr in preds.values()]):
            s = [p]
            for f in formats:
                s = f(s)
            print(*s)

json_options = {'object_pairs_hook': odict }

def parse_json(f):
    return json.loads(f, **json_options)

def load_json(path):
    with path.open('r') as f:
        return json.load(f, **json_options)

def write_json(data, path):
    log('writing %s' % path)
    with path.open('w') as f:
        json.dump(data, f)
    

def load_repo(repo_path):
    data = []
    if not repo_path.exists():
        log('Skipping repo ' + str(repo_path) + ', because file doesn\'t exist.')
        return []
    data = load_json(repo_path)
    return data
    
def write_repo(repo_path, repo_data):
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    with repo_path.open('w') as f:
        json.dump(repo_data, f)


def repo_format(data):
    return odict([(d['name'], d) for d in data])

def update_repo(repo_path, repo_files_path, urls):
    for url, furl in urls:
        try:
            r = get_uri(url, binary=False)
            fr = get_uri(furl, binary=False)
            return parse_json(r), parse_json(fr)
        except FileNotFoundError:
            continue
    raise Exception('Could not load remote repository: %s' % ', '.join(urls))

def update_repos(repositories):
    paths = [repo_filepath(r) for r in repositories]
    repo_data, repo_files = odict(), odict()
    for (repo, urls), path in zip(repositories.items(), paths):
        data, files = update_repo(repo_filepath(repo), repo_files_path(repo), urls)
        for name, versions in files.items():
            for version in versions:
                data[name][version]['files'] = files[name][version]
                
        write_repo(path, data)
        repo_data[repo] = data
    return repo_data

def load_repos(repos):
    repo_data = {}
    paths = [repo_filepath(r) for r in repos]
    data = [load_repo(p) for p in paths]
    return odict(zip(repos.keys(), data))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="A plugin/handler based package manager")
    parser.add_argument('--update', '-y', action='store_true', help="Update package lists.")

    subparsers = parser.add_subparsers(help='sub-command help')
    add_p = subparsers.add_parser('add', help='Add something to the package index')
    add_p.add_argument('--repo', '-r', help="Repository file to add package to")
    add_p.add_argument('paths', nargs='+', help="A directory with the files for each package",type=Path)
    add_p.add_argument('--force', '-f', action='store_true', help="Force updating of package")
    add_p.set_defaults(func=add)
    install_p = subparsers.add_parser('install', help='install a package')
    install_p.add_argument('packages', nargs='+', help="Packages to work on.")
    install_p.add_argument('--force', '-f', action='store_true', help="Force overwriting of existing files")
    install_p.set_defaults(func=install)

    remove_p = subparsers.add_parser('remove', help='Remove a package')
    remove_p.add_argument('packages', nargs='+', help="Packages to work on.")
    remove_p.set_defaults(func=remove)
    #  remove_p.add_argument('-b', '--dbpath', help='set an alternate database location', action='store_true')
    #  remove_p.add_argument('-c', '--cascade', help='remove packages and all packages that depend on them', action='store_true')
    #  remove_p.add_argument('-d', '--nodeps', help='skip dependency version checks (-dd to skip all checks)', action='store_true')
    #  remove_p.add_argument('-n', '--nosave', help='remove configuration files', action='store_true')
    #  remove_p.add_argument('-p', '--print', help='print the targets instead of performing the operation', action='store_true')
    #  remove_p.add_argument('-r', '--root', help='set an alternate installation root', action='store_true')
    #  remove_p.add_argument('-s', '--recursive', help='remove unnecessary dependencies', action='store_true')
    #  remove_p.add_argument('-ss', '--recursive', help='also remove explicitly installed dependencies', action='store_true')
    remove_p.add_argument('--unneeded', '-u', help='remove unneeded packages', action='store_true')
    
    list_p = subparsers.add_parser('list', help='List packages in the index')

    list_p.add_argument('--all', '-a', action='store_true', help="List all packages instead of only installed")
    list_p.add_argument('-d', '--deps', help='list packages installed as dependencies [filter]', action='store_true')
    list_p.add_argument('-e', '--explicit', help='list packages explicitly installed [filter]', action='store_true')
#    list_p.add_argument('-g', '--groups', help='view all members of a package group', action='store_true')
#    list_p.add_argument('-i', '--info', help='view package information (-ii for backup files)', action='store_true')
#    list_p.add_argument('-k', '--check', help='check that package files exist (-kk for file properties)', action='store_true')
#    list_p.add_argument('-l', '--list', help='list the files owned by the queried package', action='store_true')
#    list_p.add_argument('-m', '--foreign', help='list installed packages not found in sync db(s) [filter]', action='store_true')
#    list_p.add_argument('-n', '--native', help='list installed packages only found in sync db(s) [filter]', action='store_true')
#    list_p.add_argument('-o', '--owns', help='query the package that owns <file>', action='store_true')
#    list_p.add_argument('-p', '--file', help='query a package file instead of the database')
#    list_p.add_argument('-q', '--quiet', help='show less information for query and search')
#    list_p.add_argument('-r', '--root', help='set an alternate installation root')
#    list_p.add_argument('-s', '--search', help='search locally-installed packages for matching strings')
#    list_p.add_argument('-t', '--unrequired', help='list packages not (optionally) required by any package (-tt to ignore optdepends) [filter]')
#    list_p.add_argument('-u', '--upgrades', help='list outdated packages [filter]')
  
    list_p.set_defaults(func=list_packages)
    
    args = parser.parse_args()

    if args.update:
        repos_filepath.mkdir(parents=True, exist_ok=True)

        repo_data = update_repos(repositories)
    else:
        repo_data = load_repos(repositories)

    package_data = odict()
    for repo, data in repo_data.items():
        package_data.update(data)

    if 'func' not in args:
        parser.print_help()
        exit(1)
    args.func(args, package_data, repo_data)
    
