import argparse
import logging
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import PurePath
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Union

import yaml
from plexapi.server import PlexServer

PathLike = Union[PurePath, os.PathLike]
PlexLibrary = NamedTuple("PlexLibrary", [("src", PathLike), ("dest", PathLike)])
PlexHost = NamedTuple("PlexHost", [("host", str), ("port", int), ("token", str)])

logger = logging.getLogger("refresh_plex")


@dataclass
class SyncMetric:
    dirs: int = 0
    files: int = 0


class Config:
    def __init__(self, parsed_args):
        self.config_file: PathLike = PurePath(parsed_args.config)
        self.plex_libs: List[PlexLibrary] = []
        self.plex: Optional[PlexHost] = None
        self.dry_run: bool = parsed_args.dry_run
        self.skip_plex_scan: bool = parsed_args.skip_plex_scan
        self.verbosity: str = parsed_args.verbosity

    def parse_config_file(self):
        with open(self.config_file) as fp:
            config_dict = yaml.safe_load(fp)
        for lib_dict in config_dict["libs"]:
            lib = PlexLibrary(PurePath(lib_dict["src"]), PurePath(lib_dict["dest"]))
            is_valid = True
            if not os.path.exists(lib.src):
                is_valid = False
                logger.error(f"lib src is missing: {lib.src}")
            if not os.path.exists(lib.dest):
                is_valid = False
                logger.error(f"lib dest is missing: {lib.dest}")
            if is_valid:
                self.plex_libs.append(lib)
        plex_dict = config_dict.get("plex")
        self.plex = PlexHost(
            plex_dict.get("host", "localhost"),
            plex_dict.get("port", 32400),
            plex_dict["token"],
        )


class Plex:
    def __init__(self, config: Config):
        self.config = config
        self.plex: PlexServer = PlexServer(
            f"http://{self.config.plex.host}:{self.config.plex.port}",
            self.config.plex.token,
        )

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
        logger.debug(f"checking if {dest_path} is removed")
        if not os.path.exists(src_path):
            if is_dirs:
                if not self.config.dry_run:
                    shutil.rmtree(dest_path, ignore_errors=True)
                logger.info(f"Directory removed: {src_path}")
            else:
                if not self.config.dry_run:
                    os.remove(dest_path)
                logger.info(f"File removed: {src_path}")
            return True
        return False

    def check_added_media(
        self,
        root: str,
        name: str,
        lib_src: PathLike,
        lib_dest: PathLike,
        is_dirs,
    ) -> bool:
        src_path: PathLike = PurePath(root).joinpath(name)
        rel_path: PathLike = src_path.relative_to(lib_src)
        dest_path: PathLike = lib_dest.joinpath(rel_path)
        logger.debug(f"checking if {src_path} is added")
        if not os.path.exists(dest_path):
            if is_dirs:
                if not self.config.dry_run:
                    os.mkdir(dest_path)
                logger.info(f"Directory created: {dest_path}")
            else:
                if not self.config.dry_run:
                    os.link(src_path, dest_path)
                logger.info(f"Hardlink created: {src_path}")
            return True

        return False

    def check_changed_media(
        self,
        root: str,
        name: str,
        lib_src: PathLike,
        lib_dest: PathLike,
        is_dirs: bool,
    ) -> bool:
        if is_dirs:
            return False
        dest_path: PathLike = PurePath(root).joinpath(name)
        rel_path: PathLike = dest_path.relative_to(lib_dest)
        src_path: PathLike = lib_src.joinpath(rel_path)
        logger.debug(f"checking if {dest_path} has changed")
        src_size = os.path.getsize(src_path)
        dest_dize = os.path.getsize(dest_path)
        if src_size != dest_dize:
            if not self.config.dry_run:
                os.remove(dest_path)
                os.link(src_path, dest_path)
            logger.info(
                f"Refreshed hardlink: {src_path} [{sizeof_fmt(src_size)} => {sizeof_fmt(dest_dize)}]"
            )
            return True
        return False

    def sync(self) -> Optional[dict[str, SyncMetric]]:
        if not self.config.plex_libs:
            logger.warning("No libraries to sync")
            return None

        logger.info("Syncing libraries")

        metrics: dict[str, SyncMetric] = {
            "removed": SyncMetric(),
            "added": SyncMetric(),
            "changed": SyncMetric(),
        }

        for lib in self.config.plex_libs:
            for root, dirs, files in os.walk(lib.dest):
                for dir in dirs:
                    if self.check_removed_media(root, dir, lib.src, lib.dest, True):
                        dirs.remove(dir)
                        metrics["removed"].dirs += 1
                for file in files:
                    if self.check_removed_media(root, file, lib.src, lib.dest, False):
                        metrics["removed"].files += 1
                    elif self.check_changed_media(root, file, lib.src, lib.dest, False):
                        metrics["changed"].files += 1

            for root, dirs, files in os.walk(lib.src):
                for dir in dirs:
                    if self.check_added_media(root, dir, lib.src, lib.dest, True):
                        metrics["added"].dirs += 1
                for file in files:
                    if self.check_added_media(root, file, lib.src, lib.dest, False):
                        metrics["added"].files += 1

        for section in ["removed", "added", "changed"]:
            logger.info(
                f"{section} dirs={metrics[section].dirs}, files={metrics[section].files}"
            )

        return metrics

    def update_server(self, metrics: dict[str, SyncMetric]):
        if not metrics:
            return

        if (
            metrics["added"].dirs
            or metrics["added"].files
            or metrics["removed"].dirs
            or metrics["removed"].files
        ):
            logger.info("Refresh media triggered")
            self.plex.library.update()

        if metrics["changed"].dirs or metrics["changed"].files:
            logger.info("Analyze media triggered")
            for section in self.plex.library.sections():
                logger.debug(f"Triggering analyze media for {section.title}")
                section.analyze()


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


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
        "--skip-plex-scan", action="store_true", help="skip the plex library scan"
    )
    parser.add_argument(
        "--verbosity",
        default="info",
        help="what level of logging messages to show [debug, info (default), warning, error, critical]",
    )
    parsed_args = parser.parse_args(args_without_script)
    return Config(parsed_args)


def setup_logging(config):
    logging.basicConfig()
    logger.setLevel(config.verbosity.upper())


def run(*args_without_script: str):
    config = parse_args(args_without_script)
    setup_logging(config)
    if config.dry_run:
        logger.info("Doing a dry run, nothing is modified")
    config.parse_config_file()
    plex = Plex(config)
    metrics = plex.sync()
    plex.update_server(metrics)


if __name__ == "__main__":
    run(*sys.argv[1:])
