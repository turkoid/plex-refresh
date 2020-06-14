import argparse
import getpass
import logging
import os
import sys
from typing import Optional
from typing import Tuple

from fabric import Connection
from invoke import sudo

LIBRARIES = ["movies", "tv"]


def is_orphaned_path(root, name, plex_lib_dir, physical_lib_dir) -> Optional[str]:
    plex_path = os.path.join(root, name)
    rel_path = os.path.relpath(plex_path, start=plex_lib_dir)
    physical_path = os.path.join(physical_lib_dir, rel_path)
    logging.debug(f"checking if {plex_path} is orphaned")
    if not os.path.exists(physical_path):
        return plex_path
    return None


def is_new_path(
    root, name, physical_lib_dir, plex_lib_dir
) -> Tuple[Optional[str], Optional[str]]:
    physical_path = os.path.join(root, name)
    rel_path = os.path.relpath(physical_path, physical_lib_dir)
    plex_path = os.path.join(plex_lib_dir, rel_path)
    logging.debug(f"checking if {physical_path} is added")
    if not os.path.exists(plex_path):
        return physical_path, plex_path
    return None, Nonegit


class Config:
    def __init__(self, parsed_args):
        self.physical_media_base_dir: str = parsed_args.physical_media_base_dir
        self.plex_media_base_dir: str = parsed_args.plex_media_base_dir
        self.plex_host_string: str = parsed_args.plex_host
        self.plex_bin_dir: str = parsed_args.plex_bin_dir
        self.dry_run: bool = parsed_args.dry_run
        self.skip_plex_scan: bool = parsed_args.skip_plex_scan
        self.validate_plex_scan: bool = parsed_args.validate_plex_scan
        self.verbose: bool = parsed_args.verbose
        self.sudo_password: Optional[str] = None
        self.ssh_host: str = ""
        self.ssh_port: int = 22
        self.ssh_username: Optional[str] = None
        self.ssh_password: Optional[str] = None

    def validate(self) -> bool:
        is_valid = True
        for lib in LIBRARIES:
            for base_dir in [self.physical_media_base_dir, self.plex_media_base_dir]:
                lib_dir = os.path.join(base_dir, lib)
                if not os.path.exists(lib_dir):
                    logging.error(f"lib dir is missing: {lib_dir}")
                    is_valid = False
        return is_valid

    def prompt(self):
        if self.skip_plex_scan:
            return

        self.sudo_password = getpass.getpass("sudo password: ")
        if "@" in self.plex_host_string:
            self.ssh_username, host_port = self.plex_host_string.split("@", maxsplit=1)
        else:
            host_port = self.plex_host_string
        if ":" in host_port:
            self.ssh_host, port = host_port.rsplit(":", maxsplit=1)
            if port:
                self.ssh_port = int(port)
        else:
            self.ssh_host = host_port
        if self.ssh_host != "localhost":
            if not self.ssh_username:
                self.ssh_username = input("ssh username: ")
            self.ssh_password = getpass.getpass(
                "ssh password (default: sudo password): "
            )
            if not self.ssh_password:
                self.ssh_password = self.sudo_password


def sync_plex_libraries(config: Config):
    metrics = {}
    for lib in LIBRARIES:
        metrics[lib] = {
            "orphaned": {"dirs": 0, "files": 0},
            "added": {"dirs": 0, "files": 0},
        }
        physical_lib_dir = os.path.join(config.physical_media_base_dir, lib)
        plex_lib_dir = os.path.join(config.plex_media_base_dir, lib)
        if not os.path.exists(physical_lib_dir):
            logging.error(f"physical lib dir is missing: {physical_lib_dir}")
        if not os.path.exists(plex_lib_dir):
            logging.error(f"plex lib dir is missing: {plex_lib_dir}")

        # remove orphaned media
        lib_metrics = metrics[lib]["orphaned"]
        for root, dirs, files in os.walk(plex_lib_dir):
            for dir in dirs:
                orphaned_path = is_orphaned_path(
                    root, dir, plex_lib_dir, physical_lib_dir
                )
                if orphaned_path:
                    os.removedirs(orphaned_path)
                    if not config.dry_run:
                        dirs.remove(dir)
                    logging.info(f"Directory removed: {orphaned_path}")
                    lib_metrics["dirs"] += 1

            for file in files:
                orphaned_path = is_orphaned_path(
                    root, file, plex_lib_dir, physical_lib_dir
                )
                if orphaned_path:
                    if not config.dry_run:
                        os.remove(orphaned_path)
                    logging.info(f"File removed: {orphaned_path}")
                    lib_metrics["files"] += 1

        # add new symbolic links
        lib_metrics = metrics[lib]["added"]
        for root, dirs, files in os.walk(physical_lib_dir):
            for dir in dirs:
                physical_path, new_path = is_new_path(
                    root, dir, physical_lib_dir, plex_lib_dir
                )
                if new_path:
                    if not config.dry_run:
                        os.mkdir(new_path)
                    logging.info(f"Directory created: {new_path}")
                    lib_metrics["dirs"] += 1
            for file in files:
                physical_path, new_path = is_new_path(
                    root, file, physical_lib_dir, plex_lib_dir
                )
                if new_path:
                    if not config.dry_run:
                        os.link(physical_path, new_path)
                    logging.info(f"Hardlink created: {new_path}")
                    lib_metrics["files"] += 1

    for lib in LIBRARIES:
        logging.info(
            "{} orphaned dirs={}, files={}".format(
                lib, metrics[lib]["orphaned"]["dirs"], metrics[lib]["orphaned"]["files"]
            )
        )
        logging.info(
            "{} added dirs={}, files={}".format(
                lib, metrics[lib]["added"]["dirs"], metrics[lib]["added"]["files"]
            )
        )


def plex_scan_library(config: Config):
    plex_scanner = os.path.join(config.plex_bin_dir, "Plex Media Scanner")
    if config.dry_run:
        plex_scanner_cmd = f'"{plex_scanner}" --list'
    else:
        plex_scanner_cmd = f'"{plex_scanner}" --scan'

    disown = not config.validate_plex_scan
    if config.ssh_host == "localhost":
        logging.info(f"running locally: {plex_scanner_cmd}")
        sudo(
            plex_scanner_cmd,
            user="plex",
            password=config.sudo_password,
            hide=True,
            in_stream=False,
            disown=disown,
        )
    else:
        logging.debug(
            f"host={config.ssh_host}, port={config.ssh_port}, user={config.ssh_username}"
        )
        with Connection(
            host=config.ssh_host,
            port=config.ssh_port,
            user=config.ssh_username,
            connect_kwargs={"password": config.ssh_password},
        ) as conn:
            logging.info(f"running remotely: {plex_scanner_cmd}")
            conn.sudo(
                plex_scanner_cmd,
                user="plex",
                password=config.sudo_password,
                hide=True,
                in_stream=False,
                disown=disown,
            )


def parse_args(args_without_script) -> Config:
    parser = argparse.ArgumentParser(description="synchronizes plex media folders")
    parser.add_argument(
        "--dry-run",
        "-T",
        action="store_true",
        help="test sync without making modifications to the disk",
    )
    parser.add_argument(
        "--skip-plex-scan", action="store_true", help="skip the plex library scan"
    )
    parser.add_argument("--verbose", action="store_true", help="print debug messages")
    parser.add_argument(
        "--validate-plex-scan",
        action="store_true",
        help="will wait for plex scan to finish",
    )
    parsed_args = parser.parse_args(args_without_script)
    return Config(parsed_args)


if __name__ == "__main__":
    config = parse_args(sys.argv[1:])
    if config.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger("paramiko").setLevel(logging.ERROR)
        logging.getLogger("fabric").setLevel(logging.ERROR)
        logging.getLogger("invoke").setLevel(logging.ERROR)
    if config.dry_run:
        logging.info("Doing a dry run, nothing is modified")
    is_valid = config.validate()
    if is_valid:
        config.prompt()
    # still attempt sync even even if some lib dirs are missing
    sync_plex_libraries(config)
    if not is_valid:
        sys.exit(1)
    if not config.skip_plex_scan:
        plex_scan_library(config)
