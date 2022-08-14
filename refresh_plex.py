import argparse
import contextlib
import logging
import os
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from dataclasses import field
from pathlib import PurePath
from sqlite3 import Connection
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import yaml
from plexapi.exceptions import NotFound
from plexapi.server import PlexServer
from plexapi.video import Movie
from plexapi.video import Show
from plexapi.video import Video

PathLike = Union[PurePath, os.PathLike]

logger = logging.getLogger("refresh_plex")

CACHE_DB = "cache.db"


@dataclass
class PlexLibrary:
    type: str
    src: PathLike
    dest: PathLike


@dataclass
class PlexHost:
    host: str
    port: int
    token: str


@dataclass
class SyncMetric:
    dirs: List[PathLike] = field(default_factory=list)
    files: List[PathLike] = field(default_factory=list)

    def has_values(self):
        return bool(self.dirs or self.files)


@dataclass
class LibMetrics:
    added: SyncMetric = field(default_factory=SyncMetric)
    removed: SyncMetric = field(default_factory=SyncMetric)
    changed: SyncMetric = field(default_factory=SyncMetric)

    def __str__(self):
        sb = list()
        for attr in ["added", "removed", "changed"]:
            sb.append(
                f"{attr} dirs={len(getattr(self, attr).dirs)}, files={len(getattr(self, attr).files)}"
            )
        return "\n".join(sb)


class Config:
    def __init__(self, parsed_args):
        self.config_file: PathLike = PurePath(parsed_args.config)
        self.plex_libs: List[PlexLibrary] = []
        self.plex: Optional[PlexHost] = None
        self.dry_run: bool = parsed_args.dry_run
        self.skip_refresh: bool = parsed_args.skip_refresh
        self.skip_analyze: bool = parsed_args.skip_analyze
        self.refresh_cache: bool = parsed_args.refresh_cache
        self.verbosity: str = parsed_args.verbosity

    def parse_config_file(self):
        with open(self.config_file) as fp:
            config_dict = yaml.safe_load(fp)
        for lib_dict in config_dict["libs"]:
            lib = PlexLibrary(
                lib_dict["type"], PurePath(lib_dict["src"]), PurePath(lib_dict["dest"])
            )
            is_valid = True
            logger.debug(lib)
            if lib.type not in ["movie", "show"]:
                is_valid = False
                logger.error("lib type must be movie or show")
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
        for lib in self.plex.library.sections():
            logger.debug(f"Plex library: {lib.title} [{lib.type}]")

    def check_removed_media(
        self,
        root: str,
        name: str,
        lib_src: PathLike,
        lib_dest: PathLike,
        is_dirs: bool,
    ) -> Optional[PathLike]:
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
            return rel_path

    def check_added_media(
        self,
        root: str,
        name: str,
        lib_src: PathLike,
        lib_dest: PathLike,
        is_dirs,
    ) -> Optional[PathLike]:
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
            return rel_path

    def check_changed_media(
        self,
        root: str,
        name: str,
        lib_src: PathLike,
        lib_dest: PathLike,
        is_dirs: bool,
    ) -> Optional[PathLike]:
        if is_dirs:
            return None

        dest_path: PathLike = PurePath(root).joinpath(name)
        rel_path: PathLike = dest_path.relative_to(lib_dest)
        src_path: PathLike = lib_src.joinpath(rel_path)

        logger.debug(f"checking if {dest_path} has changed")

        src_size = os.path.getsize(src_path)
        dest_size = os.path.getsize(dest_path)
        if src_size != dest_size:
            if not self.config.dry_run:
                os.remove(dest_path)
                os.link(src_path, dest_path)
            logger.info(
                f"Refreshed hardlink: {src_path} [{sizeof_fmt(dest_size)} => {sizeof_fmt(src_size)}]"
            )
            return rel_path

    def sync(self) -> Optional[Dict[str, LibMetrics]]:
        if not self.config.plex_libs:
            logger.warning("No libraries to sync")
            return None

        logger.info("Syncing libraries")

        metrics: Dict[str, LibMetrics] = dict()

        for lib in self.config.plex_libs:
            lib_metrics = metrics.setdefault(lib.type, LibMetrics())

            for root, dirs, files in os.walk(lib.dest):
                for dir in dirs:
                    if path := self.check_removed_media(
                        root, dir, lib.src, lib.dest, True
                    ):
                        dirs.remove(dir)
                        lib_metrics.removed.dirs.append(path)
                for file in files:
                    if path := self.check_removed_media(
                        root, file, lib.src, lib.dest, False
                    ):
                        lib_metrics.removed.files.append(path)
                    elif path := self.check_changed_media(
                        root, file, lib.src, lib.dest, False
                    ):
                        lib_metrics.changed.files.append(path)

            for root, dirs, files in os.walk(lib.src):
                for dir in dirs:
                    if path := self.check_added_media(
                        root, dir, lib.src, lib.dest, True
                    ):
                        lib_metrics.added.dirs.append(path)
                for file in files:
                    if path := self.check_added_media(
                        root, file, lib.src, lib.dest, False
                    ):
                        lib_metrics.added.files.append(path)

        for lib_type, lib_metrics in metrics.items():
            logger.info(f"\n{lib_type} metrics:\n{lib_metrics}")

        return metrics

    def refresh_libraries(self):
        if self.config.skip_refresh:
            logger.warning("Skipping refresh...")
            return

        if not self.config.dry_run:
            self.plex.library.update()

        logger.info("Refresh media triggered")

    def refresh_cache(self):
        logger.info("Refreshing cache...")
        with open_db(CACHE_DB, reset=True) as conn:
            data: List[Tuple[str, int, str]] = []
            for lib_section in self.plex.library.sections():
                lib_type = lib_section.type
                if lib_type not in ["movie", "show"]:
                    continue
                for item in lib_section.all(
                    "movie" if lib_type == "movie" else "episode"
                ):
                    # for item in [lib_section.fetchItem(865)]:
                    #     print(item.ratingKey)
                    for file in [mp.file for m in item.media for mp in m.parts]:
                        data.append((lib_type, item.ratingKey, str(PurePath(file))))
            conn.executemany("INSERT INTO media VALUES (?, ?, ?)", data)
            conn.commit()

    def is_plex_media_valid(
        self, item: Union[Movie, Show], expected_media_path: str
    ) -> bool:
        for media_part in [mp for m in item.media for mp in m.parts]:
            if PurePath(media_part.file) == PurePath(expected_media_path):
                return True
        return False

    def find_items(
        self, changed_paths: Dict[str, List[PathLike]], verify: bool
    ) -> Optional[Tuple[Dict[str, Video], Dict[str, List[PathLike]]]]:
        analyze_items: Dict[str, Video] = {}
        missing_items: Dict[str, List[PathLike]] = {}

        with open_db(CACHE_DB) as conn:
            cur = conn.cursor()
            stmt = "SELECT * FROM media WHERE lib = ? AND path LIKE ?"
            for lib_type, lib_changed_paths in changed_paths.items():
                for relative_path in lib_changed_paths:
                    is_valid = False
                    for lib, key, media_path in cur.execute(
                        stmt, (lib_type, f"%{relative_path}")
                    ):
                        if key in analyze_items:
                            is_valid = True
                            continue
                        try:
                            item: Union[Movie, Show] = self.plex.library.fetchItem(
                                int(key)
                            )
                            if verify and not self.is_plex_media_valid(
                                item, media_path
                            ):
                                return None
                            is_valid = True
                            analyze_items[key] = item
                            continue
                        except (NotFound, AttributeError):
                            if verify:
                                return None
                    if not is_valid:
                        if verify:
                            return None
                        else:
                            missing_items.setdefault(lib_type, []).append(relative_path)

        return analyze_items, missing_items

    def analyze_libraries(self, changed_paths: Dict[str, List[PathLike]]):
        if self.config.skip_analyze:
            logger.warning("Skipping analyze...")
            return

        if not changed_paths:
            return

        items = self.find_items(changed_paths, not self.config.refresh_cache)
        if items is None:
            logger.warning("Cache invalid...")
            self.refresh_cache()
            analyze_items, missing_items = self.find_items(changed_paths, False)
        else:
            analyze_items, missing_items = items

        for video in analyze_items.values():
            if not self.config.dry_run:
                video.analyze()
            logger.info(f"Triggered analyze for {video}")

        for lib_type, paths in missing_items.items():
            lib_path = PurePath(f"[{lib_type}]")
            for rel_path in paths:
                fake_path = lib_path.joinpath(rel_path)
                logger.warning(f"Analyzed skipped for {fake_path}")

    def update_server(self, metrics: Dict[str, LibMetrics]):
        if not metrics:
            return

        if any(lib_metrics.changed.has_values() for lib_metrics in metrics.values()):
            self.analyze_libraries(
                {
                    lib_type: lib_metrics.changed.files
                    for lib_type, lib_metrics in metrics.items()
                }
            )

        if any(
            lib_metrics.added.has_values() or lib_metrics.removed.has_values()
            for lib_metrics in metrics.values()
        ):
            self.refresh_libraries()


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
        "--skip-refresh", action="store_true", help="skip the plex library refresh"
    )
    parser.add_argument(
        "--skip-analyze",
        action="store_true",
        help="skip triggering analyze for changed items",
    )
    parser.add_argument(
        "--refresh-cache", action="store_true", help="refresh the plex media cache"
    )
    parser.add_argument(
        "--verbosity",
        default="info",
        help="what level of logging messages to show [debug, info (default), warning, error, critical]",
    )
    parsed_args = parser.parse_args(args_without_script)
    return Config(parsed_args)


@contextlib.contextmanager
def open_db(db: str, reset: bool = False) -> Connection:
    conn = sqlite3.connect(db)
    try:
        if reset:
            conn.execute("DROP TABLE IF EXISTS media")
        conn.execute("CREATE TABLE IF NOT EXISTS media (lib TEXT, key INT, path TEXT)")
        conn.commit()
        yield conn
    finally:
        conn.close()


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
    if config.refresh_cache:
        plex.refresh_cache()
    plex.update_server(metrics)


if __name__ == "__main__":
    run(*sys.argv[1:])
