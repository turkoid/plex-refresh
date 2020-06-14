import argparse
import logging
import os
import sys
from pathlib import PurePath
from pathlib import PurePosixPath
from typing import Union


LIBRARIES = ["movies", "tv"]

PathLike = Union[PurePath, os.PathLike]


class Config:
    def __init__(self, parsed_args):
        self.src_base_dir: PurePath = PurePath(parsed_args.src_base_dir)
        self.dest_base_dir: PurePath = PurePath(parsed_args.dest_base_dir)
        self.plex_host_string: str = parsed_args.plex_host
        self.plex_bin_dir: PurePosixPath = PurePosixPath(parsed_args.plex_bin_dir)
        self.dry_run: bool = parsed_args.dry_run
        self.skip_plex_scan: bool = parsed_args.skip_plex_scan
        self.verbose: bool = parsed_args.verbose

    def validate(self) -> bool:
        is_valid = True
        for lib in LIBRARIES:
            for base_dir in [self.src_base_dir, self.dest_base_dir]:
                lib_dir: PathLike = base_dir.joinpath(lib)
                if not os.path.exists(lib_dir):
                    logging.error(f"lib dir is missing: {lib_dir}")
                    is_valid = False
        return is_valid


class Plex:
    def __init__(self, config: Config):
        self.config = config

    def check_removed_media(
        self,
        root: str,
        name: str,
        src_lib_dir: PathLike,
        dest_lib_dir: PathLike,
        is_dirs: bool,
    ) -> bool:
        dest_path: PathLike = PurePath(root).joinpath(name)
        rel_path: PathLike = dest_path.relative_to(dest_lib_dir)
        src_path: PathLike = src_lib_dir.joinpath(rel_path)
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
        self,
        root: str,
        name: str,
        src_lib_dir: PathLike,
        dest_lib_dir: PathLike,
        is_dirs,
    ) -> bool:
        src_path: PathLike = PurePath(root).joinpath(name)
        rel_path: PathLike = src_path.relative_to(src_lib_dir)
        dest_path: PathLike = dest_lib_dir.joinpath(rel_path)
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

    def sync(self):
        metrics = {}
        for lib in LIBRARIES:
            metrics[lib] = {
                "removed": {"dirs": 0, "files": 0},
                "added": {"dirs": 0, "files": 0},
            }
            src_lib_dir: PathLike = self.config.src_base_dir.joinpath(lib)
            dest_lib_dir: PathLike = self.config.dest_base_dir.joinpath(lib)
            if not os.path.exists(src_lib_dir):
                logging.error(f"src lib dir is missing: {src_lib_dir}")
            if not os.path.exists(dest_lib_dir):
                logging.error(f"dest lib dir is missing: {dest_lib_dir}")

            lib_metrics = metrics[lib]["removed"]
            for root, dirs, files in os.walk(dest_lib_dir):
                for dir in dirs:
                    if self.check_removed_media(
                        root, dir, src_lib_dir, dest_lib_dir, True
                    ):
                        dirs.remove(dir)
                        lib_metrics["dirs"] += 1
                for file in files:
                    if self.check_removed_media(
                        root, file, src_lib_dir, dest_lib_dir, False
                    ):
                        lib_metrics["files"] += 1

            lib_metrics = metrics[lib]["added"]
            for root, dirs, files in os.walk(src_lib_dir):
                for dir in dirs:
                    if self.check_added_media(
                        root, dir, src_lib_dir, dest_lib_dir, True
                    ):
                        lib_metrics["dirs"] += 1
                for file in files:
                    if self.check_added_media(
                        root, file, src_lib_dir, dest_lib_dir, False
                    ):
                        lib_metrics["files"] += 1

        for lib in LIBRARIES:
            for section in ["removed", "added"]:
                dirs_metric = metrics[lib][section]["dirs"]
                files_metric = metrics[lib][section]["files"]
                logging.info(
                    f"{lib} {section} dirs={dirs_metric}, files={files_metric}"
                )

    def scan_and_refresh(self):
        pass


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
    plex = Plex(config)
    plex.sync()
    if not is_valid:
        sys.exit(1)
    if not config.skip_plex_scan:
        plex.scan_and_refresh()
