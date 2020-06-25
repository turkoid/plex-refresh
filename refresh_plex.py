import argparse
import logging
import os
import sys
import uuid
from pathlib import PurePath
from platform import uname
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Union

import requests
import yaml

PathLike = Union[PurePath, os.PathLike]
PlexLibrary = NamedTuple("PlexLibrary", [("src", PathLike), ("dest", PathLike)])
PlexHost = NamedTuple("PlexHost", [("host", str), ("port", int), ("token", str)])


class Config:
    def __init__(self, parsed_args):
        self.config_file: PathLike = PurePath(parsed_args.config)
        self.plex_libs: List[PlexLibrary] = []
        self.plex_host: Optional[PlexHost] = None
        self.validate: bool = parsed_args.validate
        self.dry_run: bool = parsed_args.dry_run or self.validate
        self.skip_plex_scan: bool = parsed_args.skip_plex_scan
        self.verbose: bool = parsed_args.verbose

    def parse_config_file(self):
        with open(self.config_file) as fp:
            config_dict = yaml.safe_load(fp)
        for lib_dict in config_dict["libs"]:
            lib = PlexLibrary(PurePath(lib_dict["src"]), PurePath(lib_dict["dest"]))
            is_valid = True
            if not os.path.exists(lib.src):
                is_valid = False
                logging.error(f"lib src is missing: {lib.src}")
            if not os.path.exists(lib.dest):
                is_valid = False
                logging.error(f"lib dest is missing: {lib.dest}")
            if is_valid:
                self.plex_libs.append(lib)
        plex_dict = config_dict.get("plex")
        self.plex_host = PlexHost(
            plex_dict.get("host", "localhost"),
            plex_dict.get("port", 32400),
            plex_dict["token"],
        )


class Plex:
    def __init__(self, config: Config):
        self.config = config

    def check_removed_media(
        self,
        root: str,
        name: str,
        lib_src: PathLike,
        lib_dest: PathLike,
        is_dirs: bool,
    ) -> bool:
        dest_path: PathLike = PurePath(root).joinpath(name)
        rel_path: PathLike = dest_path.relative_to(lib_dest)
        src_path: PathLike = lib_src.joinpath(rel_path)
        logging.debug(f"checking if {dest_path} is removed")
        if not os.path.exists(src_path):
            if is_dirs:
                if not self.config.dry_run:
                    os.removedirs(dest_path)
                logging.info(f"Directory removed: {src_path}")
            else:
                if not self.config.dry_run:
                    os.remove(dest_path)
                logging.info(f"File removed: {src_path}")
            return True
        return False

    def check_added_media(
        self, root: str, name: str, lib_src: PathLike, lib_dest: PathLike, is_dirs,
    ) -> bool:
        src_path: PathLike = PurePath(root).joinpath(name)
        rel_path: PathLike = src_path.relative_to(lib_src)
        dest_path: PathLike = lib_dest.joinpath(rel_path)
        logging.debug(f"checking if {src_path} is added")
        if not os.path.exists(dest_path):
            if is_dirs:
                if not self.config.dry_run:
                    os.mkdir(dest_path)
                logging.info(f"Directory created: {dest_path}")
            else:
                if not self.config.dry_run:
                    os.link(src_path, dest_path)
                logging.info(f"Hardlink created: {src_path}")
            return True
        return False

    def sync(self) -> bool:
        metrics = {"removed": {"dirs": 0, "files": 0}, "added": {"dirs": 0, "files": 0}}
        for lib in self.config.plex_libs:
            lib_metrics = metrics["removed"]
            for root, dirs, files in os.walk(lib.dest):
                for dir in dirs:
                    if self.check_removed_media(root, dir, lib.src, lib.dest, True):
                        dirs.remove(dir)
                        lib_metrics["dirs"] += 1
                for file in files:
                    if self.check_removed_media(root, file, lib.src, lib.dest, False):
                        lib_metrics["files"] += 1

            lib_metrics = metrics["added"]
            for root, dirs, files in os.walk(lib.src):
                for dir in dirs:
                    if self.check_added_media(root, dir, lib.src, lib.dest, True):
                        lib_metrics["dirs"] += 1
                for file in files:
                    if self.check_added_media(root, file, lib.src, lib.dest, False):
                        lib_metrics["files"] += 1

        changed = False
        for section in ["removed", "added"]:
            dirs_metric = metrics[section]["dirs"]
            files_metric = metrics[section]["files"]
            changed = changed or dirs_metric or files_metric
            logging.info(f"{section} dirs={dirs_metric}, files={files_metric}")

        return changed

    def scan_and_refresh(self):
        plex = self.config.plex_host
        api_url = f"http://{plex.host}:{plex.port}"
        if not self.config.validate:
            api_url = f"{api_url}/library/sections/all/refresh"
        headers = {
            "X-Plex-Platform": uname()[0],
            "X-Plex-Platform-Version": uname()[2],
            "X-Plex-Provides": "controller",
            "X-Plex-Client-Identifier": str(hex(uuid.getnode())),
            "X-Plex-Product": "Plex-Refresh",
            "X-Plex-Version": "0.9b",
            "X-Plex-Device": uname()[0],
            "X-Plex-Device-Name": uname()[1],
            "X-Plex-Token": plex.token,
            "X-Plex-Sync-Version": "2",
        }

        response: requests.Response = requests.get(api_url, headers=headers)
        response_text = response.text.encode("utf-8")
        if response.ok:
            if config.dry_run:
                logging.info(f"Status {response.status_code}: {response_text}")
            else:
                logging.info("Scan and Refresh triggered")
        else:
            logging.error("Scan and Refresh failed")
            logging.error(f"Status {response.status_code}: {response_text}")


def parse_args(args_without_script) -> Config:
    parser = argparse.ArgumentParser(description="synchronizes plex media folders")
    parser.add_argument("--config", "-c", required=True, help="path to config file")
    parser.add_argument(
        "--dry-run",
        "-T",
        action="store_true",
        help="test sync without making modifications to the disk",
    )
    parser.add_argument(
        "--validate",
        "-V",
        action="store_true",
        help="verifies library paths and tests server connection",
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
    if config.dry_run:
        logging.info("Doing a dry run, nothing is modified")
    config.parse_config_file()
    plex = Plex(config)
    if config.validate:
        plex.scan_and_refresh()
    elif config.plex_libs:
        logging.info("Syncing libraries")
        if plex.sync():
            logging.info("Scanning and refreshing")
            plex.scan_and_refresh()
    else:
        logging.warning("No libraries to sync")
