import os
import argparse
from shutil import copy2
import hashlib
from datetime import datetime
import sys
import time
import logging
import re
try:
    from string import ascii_lowercase
    from string import ascii_uppercase
except ImportError:
    from string import lowercase as ascii_lowercase


MAX_LENGTH = 260


class Shortener(object):
    def __init__(self, DoR, pth, idx=1):
        self.logger = logging.getLogger("shortener")
        self.index = idx
        self.root = None
        self.root_len = 0
        self.fn = None
        self.fn_len = 0
        self.dest_root = None
        self.dest_files = None
        self.dest_metadata = None
        self.dest_len = 0
        self.dest_fn = None
        self.package_folder = None
        self.metadata_folder = 'metadata'
        self.metadata_file = None
        self.metadata_fh = None
        self.dor = DoR
        self.file_count = 0
        self.path = pth
        self.num_metadata_files = 1
        self._set_package_folder()
        self.update_folder = 0
        self._count_files()

    def _count_files(self):
        print("Counting files to be shortened.")
        for root, dirs, files in os.walk(self.path, topdown=False):
            if root != self.root:
                self.set_root(root)
                self.file_count += 1
                sys.stdout.write("%010d\r" % self.file_count)
                sys.stdout.flush()
        print("There are {} folders to be processed".format(self.file_count))
        self.root = None

    def set_root(self, path):
        self.root = path
        self.root_len = len(path)

    def set_dest_path(self, path):
        self.dest_root = path
        self.dest_metadata = os.path.join(self.dest_root, self.metadata_folder)
        self.dest_len = len(path)

    def _set_package_folder(self):
        m = hashlib.sha256(str(datetime.now()).encode("utf-8"))
        st = '{}'.format(datetime.now())
        self.package_folder = m.hexdigest()[:10]
        self.metadata_file = self.package_folder + '_{0:02d}'.format(self.num_metadata_files) + '.tsv'

    def open_new_metadata_file(self):
        self.num_metadata_files += 1
        self.metadata_file = self.package_folder + '_{0:02d}'.format(self.num_metadata_files) + '.tsv'
        self.metadata_fh = open(os.path.join(self.dest_metadata, self.metadata_file), 'a', encoding='utf-8')
        # Index \t DoR \t Original FP \t Moved FP
        self.metadata_fh.write('{}\t{}\t{}\t{}\n'.format('Index', 'DoR', 'Original File Path', 'Modified File Path'))

    def open_metadata_file(self):
        try:
            os.makedirs(self.dest_metadata)
        except FileExistsError as e:
            pass
        self.metadata_fh = open(os.path.join(self.dest_metadata, self.metadata_file), 'a', encoding='utf-8')
        # Index \t DoR \t Original FP \t Moved FP
        self.metadata_fh.write('{}\t{}\t{}\t{}\n'.format('Index', 'DoR', 'Original File Path', 'Modified File Path'))

    def reopen_current_metadata_file(self):
        self.metadata_fh = open(os.path.join(self.dest_metadata, self.metadata_file), 'a', encoding='utf-8')

    def close_metadata_file(self):
        self.metadata_fh.close()

    def mirror_dir(self, files):
        self.update_folder += 1
        dest = os.path.join(self.dest_root, self.package_folder) + "\\" + "{0:06d}".format(self.update_folder)
        os.makedirs(dest)
        for f in files:
            try:
                copy2("\\\\?\\{}\\{}".format(self.root, f), "{}\\{}".format(dest, f))
            except PermissionError as e:
                self.logger.error("Possible duplicate file: {}".format(e))
            self.metadata_fh.write('{}\t{}\t{}\t{}\n'.format(self.index,
                                                             self.dor,
                                                             "{}\{}".format(self.root, f),
                                                             "{}\{}".format(dest, f)))

    def run(self, path):
        for root, dirs, files in os.walk(path.strip(), topdown=False):
            if root != self.root:
                if len(files) == 0:
                    continue
                self.set_root(root)
                if os.path.getsize(os.path.join(self.dest_metadata, self.metadata_file)) > (1*10**6):
                    self.close_metadata_file()
                    self.open_new_metadata_file()
                else:
                    self.reopen_current_metadata_file()
                self.mirror_dir(files)
                self.close_metadata_file()
                self.file_count -= 1
                sys.stdout.write("%010d\r" % self.file_count)
                sys.stdout.flush()


class Validator(object):
    def __init__(self, path_to_validate: str) -> None:
        self.path = path_to_validate
        self.invalid_path = {}

    def run(self):
        for root, dirs, files in os.walk(self.path):
            for f in files:
                full_path = os.path.join(root, f)
                if len(full_path) > MAX_LENGTH:
                    self.invalid_path[full_path] = len(full_path)
                    print("Invalid Path ({}): {}".format(len(full_path), full_path))

    def write_report(self, file_name=None):
        fh = None
        if not self.invalid_path:
            return
        if file_name:
            fh = open("{}.tsv".format(file_name), "w", encoding="utf-8")
        else:
            fh = open("invalid_paths.tsv", "w", encoding="utf-8")

        for k, v in self.invalid_path.items():
            fh.write("{}\t{}\n".format(v, k))
        fh.close()
        self.invalid_path = {}


def build_logger():
    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.DEBUG, filename='file_name_validate_01.log', filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)


def single_path_run(source_path, dest_path):
    shtn = Shortener("{:%Y-%M-%d}".format(datetime.now()), source_path)
    shtn.set_dest_path(dest_path)
    shtn.open_metadata_file()
    print("Flattening folder structures: ")
    shtn.run(source_path)
    shtn.close_metadata_file()


def multiple_path_run(file):
    fh = open(file, "r", encoding="utf-8")
    for path in fh.readlines():
        shtn = Shortener("{:%Y-%M-%d}".format(datetime.now()), path.strip("\n"))
        shtn.set_dest_path('S:\Staging\Quarantine\FNS')
        shtn.open_metadata_file()
        print("Flattening folder structures: ")
        shtn.run(path)
        shtn.close_metadata_file()


def validate_a_path(args):
    path = args[0]
    val = Validator(path)
    print("Validating Path: {}".format(val.path))
    val.run()
    p = path.replace(os.path.sep, '_')
    p = p.split(":")[1]
    val.write_report(p[1:])


def walklevel(some_dir, level=2):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]


def find_path(path):
    l = []
    for root, dirs, __ in walklevel(path):
        for d in dirs:
            print(os.path.join(root, d))
            if re.match('^[0-9]{3,}_', d):
                l.append(os.path.join(root, d))
                continue
    return l


def arg_parse():
    parser = argparse.ArgumentParser(description='Validate or Shorten paths')
    parser.add_argument('--report', '-r', dest='report', nargs='?', default='invalid_files.tsv',
                        help='(EX: -r my_report_name.tsv)')
    parser.add_argument('--scan_path', '-sp', dest='scan_path', help='The path you want to scan')
    parser.add_argument('--shorten_single', '-ss', dest='shorten_single', action="store_true",
                        help='Switch that says to shorten the paths in the scan path')
    parser.add_argument('--shorten_multiple', '-sm', dest='shorten_multiple', action='store_true',
                        help='Requires a path to a text file with paths to be shortened on each line.')
    parser.add_argument('--shorten_destination', '-sd', nargs='?', dest='shorten_dest',
                        help='Used when shortening a path the parameter is the path where you want the output to live.')
    parsed = parser.parse_args()

    return parsed, parser


if __name__ == "__main__":
    build_logger()
    logger = logging.getLogger('main')
    args, parser = arg_parse()

    if args.shorten_single:
        single_path_run(args.scan_path, args.shorten_dest)
    elif args.shorten_multiple:
        multiple_path_run("{}\{}".format(os.getcwd(), args.scan_path))
    else:
        l = ['34904', '35000', '34001', '47163', '46135', '34973', '48925']
        potentials = find_path(args.scan_path)
        for item in l:
            for d in potentials:
                if d.__contains__(item):
                    validate_a_path([d])

