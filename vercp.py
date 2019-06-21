import sys
import os
import getpass
import shutil
import csv
from optparse import OptionParser
import cga_util

FILEVER_LEN = 5

def get_new_file_version(log_filepath,md5_str):
    max_file_version_int = -1
    storage_request_num = -1
    if os.path.exists(log_filepath):
        fid = open(log_filepath)
        in_reader = csv.reader(fid)
        for line in in_reader:
            line_dict = line_to_dict(line)
            # Keep track of the largest file version currently existing
            if line_dict['action'] == 'Starting':
                file_version_int = int(line_dict['file_version'])
                if file_version_int > max_file_version_int:
                    max_file_version_int = file_version_int
            # Record that a Starting event was logged for this md5
            if line_dict['action'] == 'Starting' and line_dict['md5'] == md5_str:
                candidate_file_version = line_dict['file_version']
                storage_request_num = line_dict['storage_request_num']
            # Record that a Complete event was logged, matching to the most recent Starting event
            elif line_dict['action'] == 'Complete' and line_dict['storage_request_num'] == storage_request_num:
                # TODO check that files actually exist, else skip this version
                new_file_version = candidate_file_version
                file_status = 'Preexisting'
                return file_status, new_file_version
    # Could not find pre-existing file that was validly logged, so it must be new.    
    new_version_int = max_file_version_int + 1
    new_version = str(new_version_int).zfill(FILEVER_LEN)
    file_status = 'New'
    return file_status, new_version


def get_new_storage_request_num(log_filepath):
    initial_version = '0'*FILEVER_LEN
    if not os.path.exists(log_filepath):
        new_version = initial_version
        return new_version
    
    fid = open(log_filepath)
    in_reader = csv.reader(fid)
    num_lines = 0
    for line in in_reader:
        last_line = line
        num_lines += 1
    if num_lines < 2:
        new_version = initial_version
        return new_version
    
    line_dict = line_to_dict(last_line)
    old_storage_request_num = line_dict['storage_request_num']
    old_storage_request_num_int = int(old_storage_request_num)
    new_storage_request_num_int = old_storage_request_num_int + 1
    new_storage_request_num = str(new_storage_request_num_int).zfill(FILEVER_LEN)
    return new_storage_request_num
    

def write_log_line(log_filepath, fields, line_dict):
    already_exists = os.path.exists(log_filepath)
    fid = open(log_filepath,'a')
    out_writer = csv.writer(fid, lineterminator='\n')
    if not already_exists:
        init_fields = ['action','log_path']
        init_line_dict = {'action':'Initializing',
                     'log_path':log_filepath,
                     }
        init_line = dict_to_line(init_fields,init_line_dict)
        out_writer.writerow(init_line)
        os.chmod(log_filepath,0666)
    line = dict_to_line(fields, line_dict)
    out_writer.writerow(line)
    fid.close()
    


def dict_to_line(fields,line_dict):
    line_list = []
    for field in fields:
        field_value = field + '=' + line_dict[field]
        line_list.append(field_value)
    return line_list


def line_to_dict(line):
    line_dict = {}
    for field_value in line:
        field_list = field_value.split('=')
        key = field_list[0]
        value = field_list[1]
        line_dict[key]=value
    return line_dict


def verstore(src, dest, tag):
    
    #TODO allow a glob for a pattern match?
    src_fullpath = os.path.realpath(os.path.abspath(os.path.expanduser(src)))
    link_data_filepath = os.path.abspath(os.path.expanduser(dest))
    link_md5_filepath = link_data_filepath + '.md5'
    store_dir = link_data_filepath + '.store'
    log_filepath = os.path.join(store_dir,'logfile.txt')
    link_dir = os.path.dirname(link_data_filepath)
    dest_filename = os.path.basename(link_data_filepath)
    
    if not os.path.exists(src_fullpath):
        raise Exception('src file not found: ' + src_fullpath)
    
    if os.path.exists(link_data_filepath) or os.path.exists(link_md5_filepath) or os.path.exists(store_dir):
        if os.path.exists(store_dir) and \
            (not os.path.exists(link_data_filepath) or not os.path.exists(link_md5_filepath)):
                #incomplete setup last time... nuke the directory
                shutil.rmtree(store_dir)
                if os.path.exists(link_data_filepath):
                    os.remove(link_data_filepath)
                if os.path.exists(link_md5_filepath):
                    os.remove(link_md5_filepath)
        
        # check that a valid existing store exists
        elif not os.path.islink(link_data_filepath) or not os.path.islink(link_md5_filepath) or \
           not os.path.isdir(store_dir) or not os.path.exists(log_filepath):
            raise Exception('existing data store corrupted at destination '+link_data_filepath)
        
    storage_request_num = get_new_storage_request_num(log_filepath)
    timestamp = cga_util.get_timestamp()
    username = getpass.getuser()
    filesize = os.path.getsize(src_fullpath)
        
    md5_str = cga_util.compute_md5(src_fullpath)
    
    (file_status,file_version) = get_new_file_version(log_filepath,md5_str)
    if file_status == 'New':
        operation = 'Copy'
        # TODO check that there is enough space to do the copy
    elif file_status == 'Preexisting':
        operation = 'Link'
    
    start_line_fields = ['action','storage_request_num','operation','file_version','md5','timestamp','size','user','original_path','tag']
    start_line_dict = {
        'action':'Starting',
        'storage_request_num':storage_request_num,
        'operation':operation,
        'file_version':file_version,
        'md5':md5_str,
        'timestamp':timestamp,
        'size':str(filesize),
        'user':username,
        'original_path':src_fullpath,
        'tag':tag,
        }
    
    cga_util.safe_make_dirs(store_dir,0775)

    write_log_line(log_filepath, start_line_fields, start_line_dict)
    
    store_data_filepath = os.path.join(store_dir,'.'.join(['verstore',file_version,dest_filename]))
    store_md5_filepath = store_data_filepath + '.md5'
    if operation == 'Copy':
        # This file contents has never been stored here
        # write the md5 and data files
        cga_util.write_string_to_file(store_md5_filepath,md5_str+'\n')
        shutil.copy(src_fullpath,store_data_filepath)
        os.chmod(store_md5_filepath,0555)
        os.chmod(store_data_filepath,0555)
        # TODO do setuid on these files
    elif operation == 'Link':
        # verify that the existing stored file is intact.
        old_md5_stored = cga_util.read_string_from_file(store_md5_filepath, 'rstrip')
        if old_md5_stored != md5_str:
            raise Exception ('Corrupted store: md5 in file does not match the expected value')
        old_md5_computed = cga_util.compute_md5(store_data_filepath)
        if old_md5_computed != md5_str:
            raise Exception ('Corrupted store: computed md5 of old data file does not match the expected value')
    else:
        raise Exception('unexpected value for operation: '+operation)
    
    # make symlinks point to the current version
    if os.path.exists(link_data_filepath):
        os.remove(link_data_filepath)
    os.symlink(store_data_filepath,link_data_filepath)
    if os.path.exists(link_md5_filepath):
        os.remove(link_md5_filepath)
    os.symlink(store_md5_filepath,link_md5_filepath)

    end_line_fields = ['action','storage_request_num']
    end_line_dict = {
        'action':'Complete',
        'storage_request_num':storage_request_num,
        }
    write_log_line(log_filepath, end_line_fields, end_line_dict)


if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option("-s", "--src", dest="src", help="Source file path, required", metavar="FILE")
    parser.add_option("-d", "--dest", dest="dest", help="Destination file path, required", metavar="FILE")
    parser.add_option("-t" ,"--tag", dest="tag", help="Version tag, required", metavar="VERSION")
    ## util_path default is set up such that we assume this is being run out of trunk/Python/versioned_store
    parser.add_option("-u", "--utils", dest="util_path", help="Path to Python/util", metavar="DIR",
                      default=os.path.join(os.path.dirname(sys.argv[0]), '..', 'util'))
    parser.set_defaults()
    (options, args) = parser.parse_args()
    ## optparse doesn't believe in the notion of 'required options' so check these manually
    if len([t for t in [options.src, options.dest, options.tag] if t is None]) > 0:
        raise Exception("src, dest and tag arguments are required")
    ## Take the util path and dynamically add in the cga_util library.
    #sys.path.append(options.util_path)
    #import cga_util
    verstore(options.src, options.dest, options.tag)
