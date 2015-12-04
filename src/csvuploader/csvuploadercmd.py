import datetime
import sys
import traceback
import uuid
from collections import namedtuple

from argh import arg, ArghParser, expects_obj

from csvuploader import HeaderCsv
from csvuploader.version import VERSION_STRING
import py.path
import appdirs
import yaml
import time
import logbook
from logbook import StreamHandler
import sqlalchemy as sa


class Watcher(object):
    def __init__(self, path, glob, seconds_to_wait):
        self.path = py.path.local(path)
        self.glob = glob
        self.seconds_to_wait = seconds_to_wait
        self.sizes = {}

    def watch(self):
        paths = self.path.visit(fil=self.path.join(self.glob).strpath)
        for p in paths:
            now = datetime.datetime.now()
            current_size = p.size()
            strpath = p.strpath

            last_size, last_update = self.sizes.get(p.strpath, (None, None))
            if last_update is None or current_size != last_size:
                self.sizes[strpath] = current_size, now
                continue
            if (now - last_update).total_seconds() > self.seconds_to_wait:
                yield p


def get_default_db_config_file():
    paths = [appdirs.user_config_dir('csvuploader', 'Janus'),
             appdirs.site_config_dir('csvuploader', 'Janus')]
    return [py.path.local(path).join('database.yaml').strpath for path in paths]


def get_db_connection_strings(*paths):
    db_connection_strings = {}
    for x in paths:
        p = py.path.local(x)
        if p.check(file=1):
            with p.open() as f:
                d = yaml.load(f)
                db_connection_strings.update(d)
    return db_connection_strings


class WatchDirectoryStructure(object):
    def __init__(self, base_path):
        self.base_path = py.path.local(base_path)
        self.pending_path = self.base_path.ensure('pending', dir=True)
        self.processing_path = self.base_path.ensure('processing', dir=True)
        self.complete_path = self.base_path.ensure('complete', dir=True)
        self.error_path = self.base_path.ensure('error', dir=True)
        self.tmp_path = self.base_path.ensure('tmp', dir=True)


UploadRequest = namedtuple('UploadRequest', ['path', 'db_name', 'schema_name', 'table_name', 'basename'])


def process(path, watch_dir, uploader, *args, **kwargs):
    pending_path = py.path.local(path)
    logbook.info("Processing {}".format(pending_path.strpath))
    db_name, schema_name, table_name, basename = (pending_path.dirpath('../..').basename,
                                                  pending_path.dirpath('..').basename,
                                                  pending_path.dirpath().basename,
                                                  pending_path.basename)
    processing_path = watch_dir.processing_path.join(db_name, schema_name, table_name, basename)
    processing_path.dirpath().ensure(dir=True)
    pending_path.move(processing_path)
    try:
        upload_request = UploadRequest(processing_path, db_name, schema_name, table_name, basename)
        new_file = uploader(upload_request, *args, **kwargs)
        if new_file:
            processing_path.remove()
            processing_path = new_file
    except Exception as e:
        error_path = watch_dir.error_path.join(db_name, schema_name, table_name, basename)
        error_path.dirpath().ensure(dir=True)
        logbook.error("Unable to process {}: {}".format(error_path.strpath, e))
        processing_path.move(error_path)
        return
    complete_path = watch_dir.complete_path.join(db_name, schema_name, table_name, basename)
    complete_path.dirpath().ensure(dir=True)
    logbook.info("Completed processing {}".format(complete_path))
    processing_path.move(complete_path)


def mk_unique_tempfile(path, ext):
    uid = "{:%Y-%m-%d-%H-%M-%S}-{}".format(datetime.datetime.now(), uuid.uuid4())
    return path.join(uid).new(ext=ext)


def upload_csv(request, tmp_dir, connection_strings):
    with request.path.open() as f:
        h = HeaderCsv.load(f)
        if h.metadata is None:
            h.metadata = {}
        h.metadata.pop('Error', None)
        h.metadata.pop('StackTrace', None)
        log = h.metadata.setdefault('log', [])
        try:
            connection_string = connection_strings[request.db_name]
            engine = sa.create_engine(connection_string)
            if engine.dialect.name == 'sqlite':
                schema_name = None
            else:
                schema_name = request.schema_name
            insp = sa.inspect(engine)
            if request.table_name.lower() not in (table_name.lower() for table_name in insp.get_table_names(schema_name)):
                raise Exception("Unable to upload {}. Table does not exist: {}.{}.{}".format(
                    request.path, request.db_name, request.schema_name, request.table_name))
            logbook.info("Uploading {} to {}.{}.{}".format(
                request.path, request.db_name, request.schema_name, request.table_name))
            df = h.df
            df.to_sql(request.table_name, engine, schema=schema_name, if_exists='append')
            log.append((datetime.datetime.now(), "Upload complete {} to {}.{}.{}".format(
                request.path, request.db_name, request.schema_name, request.table_name)))
        except Exception as e:
            log.append((datetime.datetime.now(), e.message))
            h.metadata['Error'] = e.message
            h.metadata['StackTrace'] = traceback.format_exc()
        finally:
            tmpfile = mk_unique_tempfile(tmp_dir, 'csv')
            with tmpfile.open('w') as f_out:
                h.dump(f_out)
            return tmpfile

@arg('-D',
     '--directory',
     default='.',
     help='directory to watch for csv files.')
@arg('--dbconfigfile',
     nargs='*',
     default=get_default_db_config_file(),
     help='config file containing database connection strings')
@expects_obj
def watch(args):
    db_connection_strings = get_db_connection_strings(*args.dbconfigfile)
    watch_dir = WatchDirectoryStructure(args.directory)
    logbook.info("Watch directory: {}".format(watch_dir.base_path))

    watcher = Watcher(watch_dir.pending_path, '*/*/*/*.csv', 5)
    while True:
        try:
            time.sleep(1)
            try:
                for p in watcher.watch():
                    try:
                        process(p, watch_dir, upload_csv, tmp_dir=watch_dir.tmp_path,
                                connection_strings=db_connection_strings)
                    except Exception as e:
                        logbook.error("Unexpected exception processing {}: {}".format(p.strpath, e))
            except Exception as e:
                logbook.error("Unexpected exception: {}".format(e))
        except KeyboardInterrupt:
            logbook.info("Received keyboard interrupt. Exiting.")
            break


parser = ArghParser()
parser.add_commands([watch])
parser.add_argument('--version',
                    action='version',
                    version='%(prog)s ' + VERSION_STRING)

if __name__ == '__main__':
    log_handler = StreamHandler(sys.stdout)
    with log_handler.applicationbound():
        parser.dispatch()
