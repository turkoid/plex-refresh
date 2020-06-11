import argparse
import getpass
import os
import sys

from fabric import Connection
from invoke import sudo

PHYSICAL_MEDIA_BASE_DIR = '/media/d'
PLEX_MEDIA_BASE_DIR = '/media/d/shares/public'


def is_orphaned_path(root, name, plex_lib_dir, physical_lib_dir):
    plex_path = os.path.join(root, name)
    rel_path = os.path.relpath(plex_path, start=plex_lib_dir)
    physical_path = os.path.join(physical_lib_dir, rel_path)
    if not os.path.exists(physical_path):
        return plex_path
    return None


def is_new_path(root, name, physical_lib_dir, plex_lib_dir):
    physical_path = os.path.join(root, name)
    rel_path = os.path.relpath(physical_path, physical_lib_dir)
    plex_path = os.path.join(plex_lib_dir, rel_path)
    if not os.path.exists(plex_path):
        return physical_path, plex_path
    return None, None


def parse_args(args_without_script):
    parser = argparse.ArgumentParser(
        description='synchronizes plex media folders'
    )
    parser.add_argument('--plex-host', '-s', default='localhost', help='location of plexmediaserver')
    parser.add_argument('--plex-tools-dir', '-p', default='/usr/lib/plexmediaserver',
                        help='location of the plex cli tools')
    parser.add_argument('--dry-run', action='store_true', help='test sync without making modifications to the disk')
    parsed_args = parser.parse_args(args_without_script)
    return parsed_args


def sync_plex_libraries(parsed_args):
    dry_run = parsed_args.dry_run
    for lib in ['movies', 'tv']:
        physical_lib_dir = os.path.join(PHYSICAL_MEDIA_BASE_DIR, lib)
        plex_lib_dir = os.path.join(PLEX_MEDIA_BASE_DIR, lib)

        # remove orphaned media
        for root, dirs, files in os.walk(plex_lib_dir):
            for dir in dirs:
                orphaned_path = is_orphaned_path(root, dir, plex_lib_dir, physical_lib_dir)
                if orphaned_path:
                    os.removedirs(orphaned_path)
                    if not dry_run:
                        dirs.remove(dir)
                    print(f'Directory removed: {orphaned_path}')
            for file in files:
                orphaned_path = is_orphaned_path(root, file, plex_lib_dir, physical_lib_dir)
                if orphaned_path:
                    if not dry_run:
                        os.remove(orphaned_path)
                    print(f'File removed: {orphaned_path}')

        # add new symbolic links
        for root, dirs, files in os.walk(physical_lib_dir):
            for dir in dirs:
                physical_path, new_path = is_new_path(root, dir, physical_lib_dir, plex_lib_dir)
                if new_path:
                    if not dry_run:
                        os.mkdir(new_path)
                    print(f'Directory created: {new_path}')
            for file in files:
                physical_path, new_path = is_new_path(root, file, physical_lib_dir, plex_lib_dir)
                if new_path:
                    if not dry_run:
                        os.link(physical_path, new_path)
                    print(f'Hardlink created: {new_path}')


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
        res = sudo(plex_scanner_cmd, user='plex', password=sudo_password, hide=True, in_stream=False)
    else:
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
        with Connection(host=host, port=port, user=username, connect_kwargs={'password': password}) as conn:
            res = conn.sudo(plex_scanner_cmd, user='plex', password=sudo_password, hide=True, in_stream=False)


if __name__ == '__main__':
    parsed_args = parse_args(sys.argv[1:])
    sync_plex_libraries(parsed_args)
    plex_scan_library(parsed_args)
