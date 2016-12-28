from pathlib import Path, PurePath
from common import FileInfo, hash_path, log
from zipfile import ZipFile
import shutil

class QuakeBsp:
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
                
            isdir = FileInfo.is_dir(f)
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
                        if ha == f[FileInfo.hash_key]:
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
            if FileInfo.is_dir(pinfo):
                #for subf in install_path.iterdir():
                #    if subf not in install_paths:
                #        raise Exception('File %s in %s/ was not installed, cannot delete dir' % (subf, path))
                if not len(installed_path.iterdir()):
                    to_remove.append(install_path)
            else:
                stats = install_path.stat()
                if pinfo['size'] != stats.st_size:
                    raise Exception('%s was written, but was modified' % (path))
                if pinfo[FileInfo.hash_key] != hash_path(install_path):
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

