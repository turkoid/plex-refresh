import argparse
import getpass
import logging
import os
import sys
from pathlib import PurePath
from pathlib import PurePosixPath
from typing import List
from typing import Optional
from typing import Tuple

from fabric import Connection
from invoke import sudo

LIBRARIES = ["movies", "tv"]


def is_orphaned_path(
    root: str, name: str, src_lib_dir: PurePath, dest_lib_dir: PurePath
) -> Optional[PurePath]:
    plex_path = PurePath(root).joinpath(name)
    rel_path = plex_path.relative_to(dest_lib_dir)
    physical_path = src_lib_dir.joinpath(rel_path)
    logging.debug(f"checking if {plex_path} is orphaned")
    if not os.path.exists(physical_path):
        return plex_path
    return None


def is_new_path(
    root: str, name: str, src_lib_dir: PurePath, dest_lib_dir: PurePath
) -> Tuple[Optional[PurePath], Optional[PurePath]]:
    physical_path = PurePath(root).joinpath(name)
    rel_path = physical_path.relative_to(src_lib_dir)
    plex_path = dest_lib_dir.joinpath(rel_path)
    logging.debug(f"checking if {physical_path} is added")
    if not os.path.exists(plex_path):
        return physical_path, plex_path
    return None, None


class Config:
    def __init__(self, parsed_args):
        self.src_base_dir: PurePath = PurePath(parsed_args.src_base_dir)
        self.dest_base_dir: PurePath = PurePath(parsed_args.dest_base_dir)
        self.plex_host_string: str = parsed_args.plex_host
        self.plex_bin_dir: PurePath = PurePosixPath(parsed_args.plex_bin_dir)
        self.dry_run: bool = parsed_args.dry_run
        self.skip_plex_scan: bool = parsed_args.skip_plex_scan
        self.verbose: bool = parsed_args.verbose
        self.prompt_for_passwords: List[str] = parsed_args.prompt_for_passwords
        self.sudo_password: Optional[str] = None
        self.ssh_host: str = ""
        self.ssh_port: int = 22
        self.ssh_username: Optional[str] = None
        self.ssh_password: Optional[str] = None

    def validate(self) -> bool:
        is_valid = True
        for lib in LIBRARIES:
            for base_dir in [self.src_base_dir, self.dest_base_dir]:
                lib_dir = base_dir.joinpath(lib)
                if not os.path.exists(lib_dir):
                    logging.error(f"lib dir is missing: {lib_dir}")
                    is_valid = False
        for password in self.prompt_for_passwords:
            if password not in ["sudo", "ssh"]:
                logging.error(f"only [sudo, ssh] are valid for prompt-for-passwords")
                is_valid = False
                break
        return is_valid

    def prompt(self):
        if self.skip_plex_scan:
            return

        if "sudo" in self.prompt_for_passwords:
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
            if "ssh" in self.prompt_for_passwords:
                if self.sudo_password:
                    msg = "ssh password (default: sudo password): "
                else:
                    msg = "ssh password : "
                self.ssh_password = getpass.getpass(msg)
            if not self.ssh_password:
                self.ssh_password = self.sudo_password


def sync_plex_libraries(config: Config):
    metrics = {}
    for lib in LIBRARIES:
        metrics[lib] = {
            "orphaned": {"dirs": 0, "files": 0},
            "added": {"dirs": 0, "files": 0},
        }
        physical_lib_dir = config.src_base_dir.joinpath(lib)
        plex_lib_dir = config.dest_base_dir.joinpath(lib)
        if not os.path.exists(physical_lib_dir):
            logging.error(f"physical lib dir is missing: {physical_lib_dir}")
        if not os.path.exists(plex_lib_dir):
            logging.error(f"plex lib dir is missing: {plex_lib_dir}")

        # remove orphaned media
        lib_metrics = metrics[lib]["orphaned"]
        for root, dirs, files in os.walk(plex_lib_dir):
            for dir in dirs:
                orphaned_path = is_orphaned_path(
                    root, dir, physical_lib_dir, plex_lib_dir
                )
                if orphaned_path:
                    if not config.dry_run:
                        os.removedirs(orphaned_path)
                    dirs.remove(dir)
                    logging.info(f"Directory removed: {orphaned_path}")
                    lib_metrics["dirs"] += 1

            for file in files:
                orphaned_path = is_orphaned_path(
                    root, file, physical_lib_dir, plex_lib_dir
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
    plex_scanner = config.plex_bin_dir.joinpath("Plex Media Scanner")
    if config.dry_run:
        plex_scanner_cmd = f'"{plex_scanner}" --list'
    else:
        plex_scanner_cmd = f'"{plex_scanner}" --list'

    # plex_scanner_cmd = '"touch" /root/blah.txt'
    if config.ssh_host == "localhost":
        logging.info(f"running locally: {plex_scanner_cmd}")
        res = sudo(
            plex_scanner_cmd,
            user="plex",
            password=config.sudo_password,
            hide=True,
            in_stream=False,
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
            print(config.sudo_password)
            res = conn.sudo(
                plex_scanner_cmd,
                user="plex",
                # password=config.sudo_password,
                # hide=True,
                # in_stream=False,
                pty=True,
            )
    print(res.stdout)


def parse_args(args_without_script) -> Config:
    parser = argparse.ArgumentParser(description="synchronizes plex media folders")
    parser.add_argument(
        "--src-base-dir", "-S", required=True, help="the location of the actual media",
    )
    parser.add_argument(
        "--dest-base-dir", "-D", required=True, help="the location of the links"
    )
    parser.add_argument(
        "--plex-host", "-H", default="localhost", help="location of plexmediaserver"
    )
    parser.add_argument(
        "--prompt-for-passwords",
        "-p",
        nargs="+",
        default=[],
        help="which passwords to prompt for [sudo, ssh]",
    )
    parser.add_argument(
        "--plex-bin-dir",
        "-b",
        default="/usr/lib/plexmediaserver",
        help="location of the plex binaries",
    )
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
    # sync_plex_libraries(config)
    if not is_valid:
        sys.exit(1)
    if not config.skip_plex_scan:
        plex_scan_library(config)
