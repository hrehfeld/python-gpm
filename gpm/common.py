import hashlib

def log(msg):
    print(msg)

def warn(msg):
    print('WARNING:', msg)



def hash_path(path):
    with path.open('rb') as f:
        return hash_file(f)

def hash_file(f):
    hash = hashlib.sha1()
    #todo read chunks
    hash.update(f.read())
    return hash.hexdigest()



class FileInfo:
    hash_key = 'sha1'
    
    @staticmethod
    def is_dir(pinfo):
        return pinfo['size'] == 0

