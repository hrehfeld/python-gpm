import requests
import argparse
from pathlib import PurePath, Path
import json

import semver
import hashlib
from collections import OrderedDict as odict

repo_ext = '.json'

repositories = {'packages': []}

repos_filepath = Path('repos')

class DefaultHandler:
    ext = 'zip'

quake_bsp_keys = ['title', 'zipbasedir', 'commandline', 'startmap']

class QuakeBsp(DefaultHandler):
    name = 'quake-bsp'

    
    def repo_filepath(self, repo_path):
        return 
    
    def __call__(self, repo_path, repo, data_paths, data):
        #	"type": "1",
        #	"rating": "5",
        #	"title": "The Five Rivers Land",
        #	"zipbasedir": "quoth",
        #	"commandline": "-hipnotic -game quoth",
        #	"startmap": "5rivers_e1"

        rs = []
        for p, d in zip(data_paths, data):
            qdatap = (p / self.name).with_suffix('.json')
            if not qdatap.exists():
                raise FileNotFoundError(str(qdatap))
            with qdatap.open('r') as f:
                qdata = json.load(f)
            r = odict()
            r['name'] = d['name']
            for k in quake_bsp_keys:
                if k in qdata:
                    r[k] = qdata[k]
            rs.append(r)
        return rs
                    
        

handlers = { 'model/x-quake-bsp': QuakeBsp }

def repo_filepath(repo):
    return (repos_filepath / repo).with_suffix(repo_ext)


package_keys = ['type', 'name', 'version', 'description', 'keywords', 'author', 'contributors', 'bugs', 'homepage', 'dependencies', 'files']

def log(msg):
    print(msg)

def add(args, package_data, repos):
    repo = args.repo
    repo_path = repo_filepath(repo)
    repo_data = odict()
    if repo in repos:
        repo_data = repos[repo]

    paths = args.paths

    handler_data = odict()
    for path in paths:
        pjson = path / 'package.json'

        with pjson.open('r') as f:
            data = json.load(f)
        name = data['name']

        version = data['version']

        if name in repo_data:
            if not args.force and not semver.compare(version, repo_data[name]['version']):
                raise Exception('Package exists and version is not higher than existing package')
            log('updating package ' + name)

        mimet = data.get('type', 'model/x-quake-bsp')

        assert(mimet in handlers)
        handler = handlers[mimet]()

        files = data.get('files', [])
        if files:
            files = [Path(f) for f in files]
        if not files:
            f = Path(name).with_suffix('.' + handler.ext)
            files = [f]
        real_files = [path / f for f in files]
        for f in real_files:
            if not f.exists():
                raise FileNotFoundError(str(f))
        def hash(path):
            hash = hashlib.sha1()
            with path.open('rb') as f:
                #todo read chunks
                hash.update(f.read())
            return hash.hexdigest()
        hashes = [hash(f) for f in real_files]
        filesizes = [f.stat().st_size for f in real_files]
        data['files'] = [odict([('path', str(f)), ('sha1', hash), ('size', size)])
                         for f, hash, size in zip(files, hashes, filesizes)]

        handler_data.setdefault(mimet, [])
        handler_data[mimet].append((path, data))

        p = odict()
        for k in package_keys:
            if k in data:
                p[k] = data[k]
        repo_data[name] = p

        
    for mimet, data in handler_data.items():
        handler = handlers[mimet]()
        subrepo_path = (repo_path.parent / (repo + '-' + handler.name)).with_suffix(repo_ext)
        packages = handler(subrepo_path, repo_data, *zip(*data))

        subrepo_data = repo_format(load_repo(subrepo_path))

        for d in packages:
            subrepo_data[d['name']] = d
        log('Writing ' + str(subrepo_path))
        write_repo(subrepo_path, subrepo_data.values())

    log('Writing ' + str(repo_path))
    write_repo(repo_path, repo_data.values())
    
        
        
def install(args, package_data, repos):
    packages = args.packages
    for p in packages:
        pass


def update_repo(repo_path, urls):
    for url in urls:
        r = requests.get(url)
        if r.status_code != 200:
            continue

        with repopath.open('w') as f:
            f.write(r.text)
        return r.json
    raise Exception('No response from any remote')
    
def load_repo(repo_path):
    data = []
    if not repo_path.exists():
        log('Skipping repo ' + repo + ', because file doesn\'t exist.')
    else:
        with repo_path.open('r') as f:
            data = json.load(f, object_pairs_hook=odict)
    return data
    
def write_repo(repo_path, repo_data):
    repo_path.parent.mkdir(parents=True, exist_ok=True)
    with repo_path.open('w') as f:
        json.dump(list(repo_data), f)


def repo_format(data):
    return odict([(d['name'], d) for d in data])

def handle_repos(repositories, handle):
    repo_data = {}
    for repo, urls in repositories.items():
        repopath = repo_filepath(repo)
        data = handle(repopath, urls)
        repo_data[repo] = repo_format(data)
    return repo_data

def update_repos(repos):
    return handle_repos(repos, lambda p, urls: update_repo(p, urls))

def load_repos(repos):
    return handle_repos(repos, lambda p, urls: load_repo(p))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Build the website's HTML from content/ and templates/, writing to output/.")
    parser.add_argument('--update', '-y', action='store_true', help="Update package lists.")

    subparsers = parser.add_subparsers(help='sub-command help')
    add_p = subparsers.add_parser('add', help='Add something to the package index')
    add_p.add_argument('--repo', '-r', help="Repository file to add package to")
    add_p.add_argument('paths', nargs='+', help="A directory with the files for each package",type=Path)
    add_p.add_argument('--force', '-f', action='store_true', help="Force updating of package")
    add_p.set_defaults(func=add)
    install_p = subparsers.add_parser('install', help='install a package')
    install_p.add_argument('packages', nargs='+', help="Packages to work on.")
    install_p.set_defaults(func=install)
    
    args = parser.parse_args()

    if args.update:
        repos_filepath.mkdir(parents=True, exist_ok=True)

        repo_data = update_repos(repositories)
    else:
        repo_data = load_repos(repositories)

    package_data = {}
    for repo, data in repo_data.items():
        for values in data.values():
            name = values['name']
            assert(name not in package_data)
            package_data[name] = values
                
    if 'func' not in args:
        parser.print_help()
        exit(1)
    args.func(args, package_data, repo_data)
    
