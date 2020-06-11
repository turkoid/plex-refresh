import argparse
import getpass
import logging
import os
import sys

from fabric import Connection
from invoke import sudo


def is_orphaned_path(root, name, plex_lib_dir, physical_lib_dir):
    plex_path = os.path.join(root, name)
    rel_path = os.path.relpath(plex_path, start=plex_lib_dir)
    physical_path = os.path.join(physical_lib_dir, rel_path)
    logging.debug(f'checking if {plex_path} is orphaned')
    if not os.path.exists(physical_path):
        return plex_path
    return None


def is_new_path(root, name, physical_lib_dir, plex_lib_dir):
    physical_path = os.path.join(root, name)
    rel_path = os.path.relpath(physical_path, physical_lib_dir)
    plex_path = os.path.join(plex_lib_dir, rel_path)
    logging.debug(f'checking if {physical_path} is added')
    if not os.path.exists(plex_path):
        return physical_path, plex_path
    return None, None


def parse_args(args_without_script):
    parser = argparse.ArgumentParser(
        description='synchronizes plex media folders'
    )
    parser.add_argument('--physical-media-base-dir', '-r', required=True, help='the location of the actual media')
    parser.add_argument('--plex-media-base-dir', '-l', required=True, help='the location of the links')
    parser.add_argument('--plex-host', '-s', default='localhost', help='location of plexmediaserver')
    parser.add_argument('--plex-tools-dir', '-p', default='/usr/lib/plexmediaserver',
                        help='location of the plex cli tools')
    parser.add_argument('--dry-run', action='store_true', help='test sync without making modifications to the disk')
    parser.add_argument('--skip-refresh', action='store_true', help='skip the plex library refresh')
    parser.add_argument('--verbose', action='store_true', help='print debug messages')
    parsed_args = parser.parse_args(args_without_script)
    return parsed_args


def sync_plex_libraries(parsed_args):
    dry_run = parsed_args.dry_run
    physical_media_base_dir = parsed_args.physical_media_base_dir
    plex_media_base_dir = parsed_args.plex_media_base_dir
    metrics = {}
    is_missing_libs = False
    for lib in ['movies', 'tv']:
        metrics[lib] = {
            'orphaned': {'dirs': 0, 'files': 0},
            'added': {'dirs': 0, 'files': 0}
        }
        physical_lib_dir = os.path.join(physical_media_base_dir, lib)
        plex_lib_dir = os.path.join(plex_media_base_dir, lib)
        if not os.path.exists(physical_lib_dir):
            logging.error(f'physical lib dir is missing: {physical_lib_dir}')
            is_missing_libs = True
        if not os.path.exists(plex_lib_dir):
            logging.error(f'plex lib dir is missing: {plex_lib_dir}')
            is_missing_libs = True

        # remove orphaned media
        lib_metrics = metrics[lib]['orphaned']
        for root, dirs, files in os.walk(plex_lib_dir):
            for dir in dirs:
                orphaned_path = is_orphaned_path(root, dir, plex_lib_dir, physical_lib_dir)
                if orphaned_path:
                    os.removedirs(orphaned_path)
                    if not dry_run:
                        dirs.remove(dir)
                    logging.info(f'Directory removed: {orphaned_path}')
                    lib_metrics['dirs'] += 1

            for file in files:
                orphaned_path = is_orphaned_path(root, file, plex_lib_dir, physical_lib_dir)
                if orphaned_path:
                    if not dry_run:
                        os.remove(orphaned_path)
                    logging.info(f'File removed: {orphaned_path}')
                    lib_metrics['files'] += 1

        # add new symbolic links
        lib_metrics = metrics[lib]['added']
        for root, dirs, files in os.walk(physical_lib_dir):
            for dir in dirs:
                physical_path, new_path = is_new_path(root, dir, physical_lib_dir, plex_lib_dir)
                if new_path:
                    if not dry_run:
                        os.mkdir(new_path)
                    logging.info(f'Directory created: {new_path}')
                    lib_metrics['dirs'] += 1
            for file in files:
                physical_path, new_path = is_new_path(root, file, physical_lib_dir, plex_lib_dir)
                if new_path:
                    if not dry_run:
                        os.link(physical_path, new_path)
                    logging.info(f'Hardlink created: {new_path}')
                    lib_metrics['files'] += 1

    for lib in ['movies', 'tv']:
        logging.info(
            '{} orphaned dirs={}, files={}'.format(lib, metrics[lib]['orphaned']['dirs'],
                                                   metrics[lib]['orphaned']['files']))
        logging.info(
            '{} added dirs={}, files={}'.format(lib, metrics[lib]['added']['dirs'], metrics[lib]['added']['files']))

    if is_missing_libs:
        sys.exit(1)


def plex_scan_library(parsed_args):
    host = parsed_args.plex_host
    plex_tools_dir = parsed_args.plex_tools_dir
    plex_scanner = os.path.join(plex_tools_dir, 'Plex Media Scanner')
    sudo_password = getpass.getpass('sudo password: ')
    if parsed_args.dry_run:
        plex_scanner_cmd = f'"{plex_scanner}" --list'
    else:
        plex_scanner_cmd = f'"{plex_scanner}" --scan'

    if host == 'localhost':
        logging.debug('running scan locally')
        sudo(plex_scanner_cmd, user='plex', password=sudo_password, hide=True, in_stream=False)
    else:
        logging.debug('running scan remotely')
        if '@' in host:
            username, host_port = host.split('@', maxsplit=1)
        else:
            username = input('ssh username: ')
            host_port = host
        password = getpass.getpass('ssh password: ')
        if not password:
            password = sudo_password
        if ':' in host_port:
            host, port = host.rsplit(':', maxsplit=1)
        else:
            host = host_port
            port = 22
        logging.debug(f'host={host}, port={port}, user={username}')
        with Connection(host=host, port=port, user=username, connect_kwargs={'password': password}) as conn:
            conn.sudo(plex_scanner_cmd, user='plex', password=sudo_password, hide=True, in_stream=False)


if __name__ == '__main__':
    parsed_args = parse_args(sys.argv[1:])
    if parsed_args.dry_run:
        logging.info('Doing a dry run, nothing is modified')
    if parsed_args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger('paramiko').setLevel(logging.ERROR)
    sync_plex_libraries(parsed_args)
    if not parsed_args.skip_refresh:
        plex_scan_library(parsed_args)
