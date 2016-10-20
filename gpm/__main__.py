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

date_format = '%Y-%m-%d %H:%M:%S %z'

default_type_data = {'quake-bsp': {}}

repo_ext = '.json'
state_ext = '.json'

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
    return (installed_state_dir / name).with_suffix(state_ext)

def package_backup_file(name):
    return (installed_state_dir / name).with_suffix('.package')


repositories = {'quaddicted': []}

repos_filepath = Path('repos')

repo_dl_dir = "https://www.quaddicted.com/filebase/"

def repo_download_url(package, path):
    return repo_dl_dir + path

cache_dir = Path('/tmp/ppm/')
backup_dir = cache_dir / '.backup'

def cache_path(package, path=None):
    r = cache_dir / (package['name'] + '-' + package['version'])
    if not path:
        return r
    return r / path

def cache_current(package, path):
    return cache_path(package, path).exists()

package_keys = ['type', 'name', 'version', 'description', 'keywords', 'author', 'contributors', 'bugs', 'homepage', 'dependencies', 'files']

hash_key = 'sha1'




def hash_path(path):
    with path.open('rb') as f:
        return hash_file(f)

def hash_file(f):
    hash = hashlib.sha1()
    #todo read chunks
    hash.update(f.read())
    return hash.hexdigest()



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
        return [Path(name).with_suffix('.' + self.ext)]
        

class QuakeBsp(DefaultHandler):
    name = 'quake-bsp'

    quake_path = Path('/home/hrehfeld/projects/quake/')
    package_keys = ['title', 'zipbasedir', 'commandline', 'startmap']
    
    def add(self, repo_data, datas):
        rs = []
        for data in datas:
            qdata = data['type'][self.name]
            
            for k in list(qdata.keys()):
                if k not in self.package_keys:
                    del (qdata[k])
                    #raise Exception('Illegal key: %s' % k)
                    

    def install(self, p, cachep, force_write):
        name = p['name']

        subp = p['type'][self.name]
        basedir = subp.get('zipbasedir', 'id1')

        qpath = self.quake_path
        #todo handle
        assert(qpath.exists())

        def copy(force_write, dryrun, dlfd, path, f):
            installp = qpath / basedir / path
            relp = PurePath(basedir) / path
            def write():
                if not dry_run:
                    installp.parent.mkdir(parents=True, exist_ok=True)
                    if isdir:
                        #create dirs as well
                        print('creating dir %s' % installp)
                        installp.mkdir()
                    else:
                        #write file
                        with installp.open('wb') as fd:
                            print('writing %s' % installp)
                            shutil.copyfileobj(dlfd, fd)
                return path
            
            def backup():
                bp = backup_dir / relp
                bp.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(installp), str(bp))
                
            isdir = hash_key not in f
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
        for path, f in p['files'].items():
            if f['subfiles']:
                cpath = (cachep / path)
                #print('opening %s' % cpath)
                with ZipFile(str(cpath), 'r') as zf:
                    for subpath, subf in f['subfiles'].items():
                        add(copy(force_write, dry_run, zf.open(subpath, 'r'), Path(subpath), subf))
            else:
                add(copy(force_write, dry_run, Path(path), f))
        
        print('wrote: %s' % written_files)
        print('errots: %s' % errors)
        return {'written': written_files}

handlers = { QuakeBsp.name: QuakeBsp }

def repo_filepath(repo):
    return (repos_filepath / repo).with_suffix(repo_ext)

def subrepo_path(repo, handler):
    return (repos_filepath / (repo + '-' + handler.name)).with_suffix(repo_ext)


def log(msg):
    print(msg)

def add_s(force, paths, packages, package_data, repo_data):
    handler_data = odict()
    for path, data in zip(paths, packages):
        name = data['name']

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
            
        data['files'] = file_infos

        for t in types:
            handler_data.setdefault(t, [])
            handler_data[t].append(data)

        p = odict()
        for k in package_keys:
            if k in data:
                p[k] = data[k]
        repo_data[name] = p

        
    for t, data in handler_data.items():
        handler = handlers[t]()
        handler.add(repo_data, data)
    

def add(args, package_data, repos):
    repo = args.repo
    repo_data = odict()
    if repo in repos:
        repo_data = repos[repo]

    paths = args.paths

    for path in paths:
        pjson = path / 'package.json'

        with pjson.open('r') as f:
            data = json.load(f)

            add_s(args.force, [path], [data], package_data, repo_data)

    repo_path = repo_filepath(repo)
    log('Writing ' + str(repo_path))
    write_repo(repo_path, repo_data.values())
    
def make_dirs(p):
    p.mkdir(parents=True, exist_ok=True)
    
        
def install(args, package_data, repo_data):
    make_dirs(installed_state_dir)

    installed_packages = load_installed_state()
    packages = args.packages
    for name in packages:
        if name not in package_data:
            raise Exception('No such package: ' + name)
        p = package_data[name]
        if name in installed_packages:
            oldp = load_json(package_backup_file(name))
            print(p, oldp)
            if semver.compare(p['version'], oldp['version']) != 1:
                raise Exception('Package %s already installed at version %s (trying: %s)' %  (name, oldp['version'], p['version']))
        as_dependency = False
        with package_backup_file(name).open('w') as f:
            json.dump(p, f)

        cachedirp = cache_path(p)
        files = p['files']
        for path, f in files.items():
            cachep = cache_path(p, path)
            if cache_current(p, path):
                continue
            url = repo_download_url(p, path)
            print('Downloading %s...' % url)
            r = requests.get(url)
            if r.status_code != 200:
                raise Exception('Could not download file: %i (%s)' % (r.status_code, url))

            hash = hashlib.sha1()
            hash.update(r.content)
            ha = hash.hexdigest()
            if ha != f[hash_key]:
                raise Exception('Download for %s was broken (failed hash).' % url)

            cachep.parent.mkdir(parents=True, exist_ok=True)
            with cachep.open('wb') as f:
                f.write(r.content)


        types = p['type']
        state = odict()
        for t in types:
            handler = handlers[t]()
            state[t] = handler.install(p, cachedirp, args.force)

        with package_state_path(name).open('w') as f:
            json.dump(state, f)
        installed_packages[name] = odict([('date', datetime.datetime.now(datetime.timezone.utc).strftime(date_format))
                                          , ('as_dependency', as_dependency )])
    write_installed_state(installed_packages)

def list_packages(args, package_data, repo_data):
    installed_packages = load_installed_state()
    for p in package_data:
        s = [p]
        if p in installed_packages:
            s.append('[installed]')
        print(*s)

def update_repo(repo_path, urls):
    for url in urls:
        r = requests.get(url)
        if r.status_code != 200:
            continue

        with repopath.open('w') as f:
            f.write(r.text)
        return r.json
    raise Exception('No response from any remote. Tried: ' + ', '.join(urls) + '.')

def load_json(path):
    with path.open('r') as f:
        return json.load(f, object_pairs_hook=odict)

def load_repo(repo_path):
    data = []
    if not repo_path.exists():
        log('Skipping repo ' + str(repo_path) + ', because file doesn\'t exist.')
    else:
        data = load_json(repo_path)
    return data
    
def write_repo(repo_path, repo_data):
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    with repo_path.open('w') as f:
        json.dump(list(repo_data), f)


def repo_format(data):
    return odict([(d['name'], d) for d in data])

def update_repos(repositories):
    repo_data = {}
    for repo, urls in repositories.items():
        repopath = repo_filepath(repo)
        data = update_repo(repopath, urls)
        repo_data[repo] = repo_format(data)
    return repo_data

def load_repos(repos):
    repo_data = {}
    for repo in repositories:
        repopath = repo_filepath(repo)
        data = load_repo(repopath)
        repo_data[repo] = repo_format(data)
    return repo_data

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
    list_p = subparsers.add_parser('list', help='List packages in the index')
    list_p.set_defaults(func=list_packages)
    
    args = parser.parse_args()

    if args.update:
        repos_filepath.mkdir(parents=True, exist_ok=True)

        repo_data = update_repos(repositories)
    else:
        repo_data = load_repos(repositories)

    package_data = odict()
    for repo, data in repo_data.items():
        for values in data.values():
            name = values['name']
            assert(name not in package_data)
            package_data[name] = values
                
    if 'func' not in args:
        parser.print_help()
        exit(1)
    args.func(args, package_data, repo_data)
    
