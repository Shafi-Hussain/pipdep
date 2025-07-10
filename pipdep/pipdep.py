import re
import sys
from datetime import datetime
import json
import logging
import os
import sys
from argparse import ArgumentParser
from asyncio import run
from collections import deque
from copy import deepcopy
from datetime import datetime
from re import finditer
from struct import unpack
from typing import Optional, Deque
from urllib.parse import urljoin, urlparse
from zlib import decompress, MAX_WBITS

from aiohttp import ClientSession, BasicAuth
from bs4 import BeautifulSoup
from bs4 import Tag as BSTag
from packaging.markers import Marker, default_environment
from packaging.metadata import parse_email
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.tags import sys_tags, Tag, parse_tag
from packaging.utils import canonicalize_name, NormalizedName, parse_wheel_filename, parse_sdist_filename, \
    InvalidSdistFilename
from packaging.version import Version
from yaml import safe_load

_ZIP_CD_START = b"\x50\x4b\x01\x02"
_ZIP_CD_END = b"\x50\x4b\x05\x06"

_FILENAME = "METADATA"
_FILENAME_ENC = _FILENAME.encode()


def _merger_markers(a: Marker, b: Marker):
    if not a:
        return b
    if not b:
        return a
    return Marker(f"{a} and {b}")


def _merge_specifiers(a: SpecifierSet, b: SpecifierSet):
    if not a:
        return b
    if not b:
        return a
    return SpecifierSet(f"{a},{b}")

# CRITICAL = 50
# FATAL = CRITICAL
# ERROR = 40
# WARNING = 30
# WARN = WARNING
# INFO = 20
# DEBUG = 10
# NOTSET =
# Accepted 0,1(HIGHEST verbosity),2,3(DEFAULT),4,5(LOWEST verbosity)
LOG_LEVEL = int(os.getenv("LOG_LEVEL", "3").strip()) * 10

# PYPI = "https://pypi.org/project/{}"
PYPI = "https://pypi.org/pypi/{}/json"
PYPI_SIMPLE = "https://pypi.org/simple/{}"

logger = logging.getLogger(__name__)
logfile = datetime.now().strftime("log_%Y-%m-%d_%H-%M-%S.log")
# logging.basicConfig(filename=logfile, level=LOG_LEVEL)
stream_handler = logging.StreamHandler(sys.stdout)
file_handler = logging.FileHandler(logfile)

formatter = logging.Formatter('[%(asctime)s] : [%(levelname)s]: %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

file_handler.setLevel(LOG_LEVEL)
stream_handler.setLevel(LOG_LEVEL)

logger.addHandler(file_handler)
# logger.addHandler(stream_handler)

logger.setLevel(LOG_LEVEL)
'''
--- Comment to myself --- 
What's the point of async queries when I am awaiting each call to parse the HTML?
Fancy but stupid. Need to find a better way...

How to parallelize the BFS? 
Currently, it gets one dependency at a time and resolves it, adding all transitive
dependencies in the queue. 
I can send a batch request for transitive dependencies in each layer,
but I need some way to wait/know when everything is completed...

==== TODO():
    Switch to DFS. Resolving with BFS, I cannot go back to the parent to ease constraints
    to choose different requirements for child dependencies
'''


async def _query_helper(sess: ClientSession, pkg: NormalizedName, server_cfg: list[dict]):
    anchor_tags = []
    for cfg in server_cfg:
        auth = None
        if "auth" in cfg:
            if cfg["auth"]["type"] == "Basic":
                auth = BasicAuth(login=cfg["auth"]["username"], password=cfg["auth"]["password"])
            elif cfg["auth"]["type"] == "EnvironmentToken":
                auth = BasicAuth(login=os.getenv(cfg["auth"]["username"]), password=os.getenv(cfg["auth"]["password"]))
        # print("Sending request to:", cfg["url"].format(pkg), "with auth", auth)
        logger.info(f"Sending request to: {cfg['url'].format(pkg)}")
        logger.debug(f"Authorization for request is: {auth}")
        async with sess.get(url=cfg["url"].format(pkg), auth=auth) as resp:
            for a_tag in BeautifulSoup(await resp.content.read(), "lxml").find_all('a'):
                logger.info(f"Found anchor tag with text: {a_tag.get_text()}")
                # if relative path, change to absolute path
                if not a_tag['href'].startswith("http"):
                    # need to figure out why artifactory links are like this...
                    if urlparse(cfg["url"]).netloc.endswith("na.artifactory.swg-devops.com"):
                        a_tag['href'] = urljoin(cfg["url"].format(pkg), a_tag['href'][1:])
                    else:
                        a_tag['href'] = urljoin(cfg["url"].format(pkg), a_tag['href'])
                anchor_tags.append(a_tag)
                logger.info(f"Anchor tag url: {a_tag['href']}")
    return anchor_tags


async def get_wheel_metadata(client: ClientSession, wheel_url: str, server_cfg: list[dict[str, str]],
                             central_dir_chunk: int = 512,
                             buffer: int = 512) -> bytes:
    auth = None
    wheel_host = urlparse(wheel_url).netloc
    for cfg in server_cfg:
        if urlparse(cfg["url"]).netloc == wheel_host:
            if "auth" in cfg:
                if cfg["auth"]["type"] == "Basic":
                    auth = BasicAuth(login=cfg["auth"]["username"], password=cfg["auth"]["password"])
                elif cfg["auth"]["type"] == "EnvironmentToken":
                    auth = BasicAuth(login=os.getenv(cfg["auth"]["username"]),
                                     password=os.getenv(cfg["auth"]["password"]))
    try:
        # Try Meta URL
        meta_url = f"{wheel_url.split('#')[0]}.metadata"
        logger.info(f"Trying to get wheel metadata from: {meta_url}")
        async with client.get(meta_url, auth=auth) as resp:
            if resp.status == 404:
                logger.info(f"HTTP 404 - for - {meta_url}")
                raise Exception()
            return await resp.content.read()
    except Exception as _:
        # if failed, get wheel metadata
        logger.info(f"Trying to parse metadata from: {wheel_url}")
        async with client.head(url=wheel_url, auth=auth) as h_request:
            h_request.raise_for_status()
            logger.debug(f"HEAD Request: headers={h_request.headers}")
            content_length = h_request.headers.get('Content-Length')

        if content_length is None:
            raise ValueError('Could not determine Content-Length')
        content_length = int(content_length)

        start_byte = max(0, content_length - central_dir_chunk * 1024)
        byte_range = f"bytes={start_byte}-{content_length - 1}"

        async with client.get(url=wheel_url, auth=auth, headers={'Range': byte_range}) as g_request:
            g_request.raise_for_status()
            wheel_cd_bytes = await g_request.content.read()
        pos = wheel_cd_bytes.find(_ZIP_CD_START)
        while pos < len(wheel_cd_bytes):
            (
                sig, ver_made, ver_needed, flags, compression, mod_time, mod_date,
                crc32, comp_size, uncomp_size, fname_len, extra_len, comment_len,
                disk_start, int_attr, ext_attr, rel_offset
            ) = unpack('<IHHHHHHIIIHHHHHII', wheel_cd_bytes[pos:pos + 46])

            filename = wheel_cd_bytes[pos + 46: pos + 46 + fname_len].decode('utf-8')
            if os.path.basename(filename) != _FILENAME:
                pos += 46 + fname_len + extra_len + comment_len
                continue
            local_header_range = f"bytes={rel_offset}-{rel_offset + 30 - 1}"
            async with client.get(url=wheel_url, auth=auth, headers={'Range': local_header_range}) as l_request:
                l_request.raise_for_status()
                local_header = await l_request.content.read()

                (
                    sig, ver_needed, flags, compression_method,
                    mod_time, mod_date, crc32_lfh, comp_size_lfh, uncomp_size_lfh,
                    fname_len_lfh, extra_len_lfh
                ) = unpack('<IHHHHHIIIHH', local_header)

            data_bytes_range = f"bytes={rel_offset + 30 + fname_len_lfh + extra_len_lfh}-{rel_offset + 30 + fname_len_lfh + extra_len_lfh + comp_size - 1}"
            async with client.get(url=wheel_url, auth=auth, headers={'Range': data_bytes_range}) as d_request:
                d_request.raise_for_status()
                data = await d_request.content.read()

                if compression_method == 8:  # DEFLATE
                    return decompress(data, wbits=-MAX_WBITS)
                elif compression_method == 0:  # STORE
                    return data
                else:
                    raise Exception(f"Unknown compression method: {compression_method}")
        raise NotADirectoryError(
            f'METADATA not found in the central directory [central_dir_chunk={central_dir_chunk}]. '
            f'Maybe try increasing he central_dir_chunk')


def build_graph(data: dict[NormalizedName, tuple[Requirement, Version | str, BSTag | None]]):
    """
    This function should build the dependency graph for a single package for a specific version
    :return:
    """
    pass


def _choose_wheels(
        wheel_tag: BSTag,
        pkg_requirement: Requirement,
        valid_profile_tags: list[Tag]
):
    name = wheel_tag.get_text(strip=True)
    # Skip SDIST
    if not name.lower().endswith(".whl"):
        return False
    pkg_name, pkg_version, bld_tag, tags = parse_wheel_filename(name)
    # print("-"*25)
    # print(pkg_name)
    # print(pkg_version)
    # print(bld_tag)
    # print(tags)
    # print("CHOSEN VERSION:", pkg_requirement.specifier.contains(pkg_version))
    # print("CHOSEN TAGS:", any(tags == r_tag for r_tag in valid_profile_tags))
    # print("-"*25)
    logger.info(f"_choose_wheels(): {wheel_tag.get_text()}")
    logger.debug(f"_choose_wheels(): Accepted for version={pkg_requirement.specifier.contains(pkg_version)}")
    logger.debug(
        f"_choose_wheels(): Accepted for tags={any(tag == r_tag for tag in tags for r_tag in valid_profile_tags)}")
    logger.info(f"_choose_wheels(): Choose wheel={pkg_requirement.specifier.contains(pkg_version) and all(tag in valid_profile_tags for tag in tags)}")
    return pkg_requirement.specifier.contains(pkg_version) \
        and any(tag == r_tag for tag in tags for r_tag in valid_profile_tags)


def _choose_wheels_from_list(
        wheel_tags_list: list[BSTag],
        pkg_requirement: Requirement,
        valid_profile_tags: list[Tag]
):
    version_matching_wheel = None
    for anchor_tag in sorted(
            filter(lambda a_tag: a_tag.get_text(strip=True).endswith(".whl"), wheel_tags_list),
            reverse=True,
            key=lambda a_tag: parse_wheel_filename(a_tag.get_text(strip=True))[1]
    ):
        _, pkg_version, _, tags = parse_wheel_filename(anchor_tag.get_text())
        # wheel version satisfies, but tags don't
        if not version_matching_wheel and pkg_requirement.specifier.contains(pkg_version) \
                and all(tag != r_tag for tag in tags for r_tag in valid_profile_tags):
            version_matching_wheel = str(pkg_version)
        if pkg_requirement.specifier.contains(pkg_version) \
                and any(tag == r_tag for tag in tags for r_tag in valid_profile_tags):
            # if higher version wheels exist but no match for platform, return none
            if version_matching_wheel and Version(version_matching_wheel) > pkg_version:
                return None
            return anchor_tag
    return None


def _choose_sdists(
        wheel_tag: BSTag,
        pkg_requirement: Requirement
):
    name = wheel_tag.get_text(strip=True).lower()
    # Skip WHEELS
    if name.endswith(".whl"):
        return False

    logger.info(f"_choose_sdists(): {wheel_tag.get_text()}")
    try:
        pkg_name, pkg_version = parse_sdist_filename(name)
        logger.debug(f"_choose_sdists(): Accepted for version={pkg_requirement.specifier.contains(pkg_version)}")
        logger.info(f"_choose_sdists(): Choose wheel={pkg_requirement.specifier.contains(pkg_version)}")
        return pkg_requirement.specifier.contains(pkg_version)
    except InvalidSdistFilename as _:
        logger.error(f"_choose_sdists(): Failed to choose sdist", exc_info=True)
        return False


def _choose_sdists_from_list(
        wheel_tags_list: list[BSTag],
        pkg_requirement: Requirement
):
    wheel_tags_list_clean = []
    for sdist in wheel_tags_list:
        try:
            parse_sdist_filename(sdist.get_text().lower())
            wheel_tags_list_clean.append(sdist)
        except InvalidSdistFilename as _:
            continue
    for anchor_tag in sorted(
            filter(lambda a_tag: not a_tag.get_text(strip=True).endswith(".whl"), wheel_tags_list_clean),
            reverse=True,
            key=lambda a_tag: parse_sdist_filename(a_tag.get_text(strip=True))[1]
    ):
        _, pkg_version = parse_sdist_filename(anchor_tag.get_text())
        if pkg_requirement.specifier.contains(pkg_version):
            return anchor_tag
    return None


def _pick_most_suitable(pkg_requirement: Requirement, wheel_urls: list[BSTag],
                        profiles: list[Tag]) -> tuple[Optional[BSTag], Optional[BSTag]]:
    wheel_filtered: Optional[BSTag] = None
    try:
        wheel_filtered = max(
            filter(lambda wheel_data: _choose_wheels(wheel_data, pkg_requirement, profiles), wheel_urls),
            key=lambda wheel_tag: parse_wheel_filename(wheel_tag.get_text(strip=True))[1]
        )
    except ValueError as _:
        logger.error(f"_pick_most_suitable(): Failed to select max() wheels", exc_info=True)
        pass
    sdist_filtered: Optional[BSTag] = None
    try:
        sdist_filtered = max(
            filter(lambda wheel_data: _choose_sdists(wheel_data, pkg_requirement), wheel_urls),
            key=lambda wheel_tag: parse_sdist_filename(wheel_tag.get_text(strip=True))[1]
        )
    except ValueError as _:
        logger.error(f"_pick_most_suitable(): Failed to select max() sdists", exc_info=True)
        pass
    return wheel_filtered, sdist_filtered


def _pick_most_suitable_from_lists(pkg_requirement: Requirement, wheel_urls: list[BSTag],
                                   profiles: list[Tag]) -> tuple[Optional[BSTag], Optional[BSTag]]:
    wheel_filtered: Optional[BSTag] = _choose_wheels_from_list(wheel_urls, pkg_requirement, profiles)
    sdist_filtered: Optional[BSTag] = _choose_sdists_from_list(wheel_urls, pkg_requirement)
    return wheel_filtered, sdist_filtered


def get_latest_irrespective_of_platform(wheel_tags_list: list[BSTag], req: Requirement):
    for anchor in sorted(
        filter(lambda a_tag: a_tag.get_text(strip=True).endswith(".whl"), wheel_tags_list),
        reverse=True,
        key=lambda a_tag: parse_wheel_filename(a_tag.get_text(strip=True))[1]
    ):
        if req.specifier.contains(parse_wheel_filename(anchor.get_text())[1]):
            return anchor
    # packages that publish only tarball - requests
    return None


async def try_finding_github(client: ClientSession, pkg_name: NormalizedName):
    '''
    TODO() -> this function needs improvement
    '''
    async with client.get(PYPI.format(pkg_name)) as resp:
        # soup = BeautifulSoup(await resp.content.read(), "lxml")
        # soup.find_all()

        # html = await resp.content.read()
        # print(html.decode())
        # gh_links = re.findall(r"https://github.com", html.decode())
        # print("GH LINKS:", gh_links)
        # if not gh_links:
        #     return None
        # # hopefully all GH links are linking to the concerned package
        # print("GH_LINK:", gh_links[0])
        # return re.match(r"https://github.com/.+/.+", gh_links[0]).group(1)
        meta = await resp.json()
        meta = meta["info"]

        possible_urls = []
        # if meta.get("project_urls") and meta.get("project_urls").get("source"):
        #     # return "/".join(meta.get("project_urls").get("source").split("/")[:5])
        #     possible_urls.append(meta.get("project_urls").get("source").strip())
        # if meta.get("project_urls") and meta.get("project_urls").get("Homepage"):
        #     # return "/".join(meta.get("project_urls").get("Homepage").split("/")[:5])
        #     possible_urls.append(meta.get("project_urls").get("Homepage").strip())
        if meta.get("project_urls"):
            for k,v in meta.get("project_urls").items():
                # if "github.com" in v:
                possible_urls.append("/".join(v.split("/")[:5]))
        if meta.get("download_url"):
            # return "/".join(meta.get("download_url").split("/")[:5])
            possible_urls.append(meta.get("download_url").strip())

        # fix this ridiculous logic
        if not possible_urls:
            return "Could not find GitHub URL"
        possible_urls = list(sorted(filter(lambda u: "github.com" in u, possible_urls)))
        if not possible_urls:
            return "Could not find GitHub URL"
        
        return "/".join(possible_urls[0].split("/")[:5])


async def find_missing(initial_requirements: list[Requirement],
                       valid_tags_profile: list[Tag],
                       base_environment: dict[str, str],
                       url_db: Optional[dict[NormalizedName, list[BSTag]]],
                       server_cfg: list[dict]):
    package_versions_resolved: dict[NormalizedName, tuple[Requirement, Version | str, BSTag | str | None]] = {}
    url_db = url_db or {}

    queue: Deque[Requirement] = deque(initial_requirements)

    async with ClientSession() as sess:
        while queue:
            pkg_req = queue.popleft()
            pkg_name = canonicalize_name(pkg_req.name)
            # print(str(pkg_name))
            logger.info(f"BFS:popped from queue: {pkg_name}")

            # exploring this package for the first time
            if pkg_name not in url_db:
                logger.info(f"BFS: {pkg_name} not found in cache")
                queried_data = await _query_helper(sess, pkg_name, server_cfg)

                logger.info(f"BFS: Add {pkg_name} to cache")
                url_db[pkg_name] = queried_data

                logger.info(f"BFS: Choose best for {pkg_name}")
                # whl, sdst = _pick_most_suitable(pkg_req, url_db[pkg_name], valid_tags_profile)
                whl, sdst = _pick_most_suitable_from_lists(pkg_req, url_db[pkg_name], valid_tags_profile)
                logger.info(f"BFS: Best {pkg_name} wheel is {whl.get_text() if whl else whl}")
                logger.info(f"BFS: Best {pkg_name} sdist is {sdst.get_text() if sdst else sdst}")
                whl_version, sdst_version = None, None
                if whl:
                    _, whl_version, _, _ = parse_wheel_filename(whl.get_text(strip=True))
                if sdst:
                    _, sdst_version = parse_sdist_filename(sdst.get_text(strip=True))
                if whl is None and sdst is None:
                    # TODO(): Find out the Github
                    package_versions_resolved[pkg_name] = (pkg_req, f"GITHUB-{pkg_req.specifier}",
                                                           await try_finding_github(sess, pkg_name))
                    pass
                else:
                    if whl is None:
                        # TODO(): Only SDIST is available...try to figure out dependencies...
                        # Some packages like torchvision may have older sdists than wheels
                        # they do not publish sdist
                        latest_whl = get_latest_irrespective_of_platform(url_db[pkg_name], pkg_req)
                        if latest_whl and parse_wheel_filename(latest_whl.get_text())[1] > sdst_version:
                            ## FETCH from GITHUB
                            package_versions_resolved[pkg_name] = (pkg_req, f"GITHUB-{pkg_req.specifier}",
                                                                   await try_finding_github(sess, pkg_name))
                        else:
                            package_versions_resolved[pkg_name] = (pkg_req, f"SDIST-{sdst_version}", sdst)
                        pass
                    # not sure if such a scenario does exist, but just to be sure
                    # scenario: wheel is available but no sdist is available
                    # UPDATE: This scenario may exist in custom pypi indices
                    elif sdst is None:
                        package_versions_resolved[pkg_name] = (pkg_req,
                                                               parse_wheel_filename(whl.get_text(strip=True))[1], whl)
                        # print("ADDED", pkg_name, "to dict")
                        logger.info(f"Adding {pkg_name} to cache")

                        # add transitive dependencies
                        wheel_metadata_bytes = await get_wheel_metadata(sess, whl['href'], server_cfg)
                        raw, unparsed = parse_email(wheel_metadata_bytes)
                        if 'requires_dist' in raw:
                            for dep in raw['requires_dist']:
                                # print("transitive:", dep)
                                logger.info(f"Identified transitive dependency: {dep}")
                                transitive_dep = Requirement(dep)
                                if transitive_dep.marker:
                                    # TODO(): all() or any()? How are extras evaluated????
                                    # print("Marker Match:", any(transitive_dep.marker.evaluate(base_environment | {"extra": extra}) for extra in pkg_req.extras))
                                    if any(transitive_dep.marker.evaluate({"extra": extra}) for extra in
                                           pkg_req.extras):
                                        queue.append(transitive_dep)
                                    elif transitive_dep.marker.evaluate(base_environment):
                                        queue.append(transitive_dep)
                                else:
                                    queue.append(transitive_dep)
                        pass
                    # tricky scenario. pip prefers latest version
                    # if sdist is newer than whl, pip will use this
                    # when will this scenario trigger? - custom index has older wheels,
                    # upstream pypi has newer wheels(unsupported platform) and newer sdist
                    else:
                        if sdst_version > whl_version:
                            # only if we have a later sdist, pip will try to compile that
                            package_versions_resolved[pkg_name] = (pkg_req, f"SDIST-{sdst_version}", sdst)
                            pass
                        else:
                            # yayyy we have a wheel
                            package_versions_resolved[pkg_name] = (pkg_req,
                                                                   parse_wheel_filename(whl.get_text(strip=True))[1],
                                                                   whl)
                            # print("ADDED", pkg_name, "to dict")
                            logger.info(f"Adding {pkg_name} to cache")
                            # add transitive dependencies
                            wheel_metadata_bytes = await get_wheel_metadata(sess, whl['href'], server_cfg)
                            raw, unparsed = parse_email(wheel_metadata_bytes)
                            if 'requires_dist' in raw:
                                for dep in raw['requires_dist']:
                                    # print("transitive:", dep)
                                    logger.info(f"Identified transitive dependency: {dep}")
                                    transitive_dep = Requirement(dep)
                                    if transitive_dep.marker:
                                        # TODO(): all() or any()? How are extras evaluated????
                                        # print("Marker Match:", any(transitive_dep.marker.evaluate(base_environment | {"extra": extra}) for extra in pkg_req.extras))
                                        if any(transitive_dep.marker.evaluate({"extra": extra}) for extra in
                                               pkg_req.extras):
                                            queue.append(transitive_dep)
                                        elif transitive_dep.marker.evaluate(base_environment):
                                            queue.append(transitive_dep)
                                    else:
                                        queue.append(transitive_dep)

                # if whl is None and sdst is None:
                #     # TODO(): FIND GITHUB URL ??
                #     # print("WHEEL OR SDIST not available")
                #     package_versions_resolved[pkg_name] = (pkg_req, "GITHUB")
                #     pass
                # elif whl is not None:
                #     # choose this wheel for now
                #     # might change based on future explorations
                #     # print("WHEEL AVAILABLE, add to dict")
                #     package_versions_resolved[pkg_name] = (pkg_req, parse_wheel_filename(whl.get_text(strip=True))[1])
                #     print("ADDED", pkg_name, "to dict")
                #
                #     # add transitive dependencies
                #     wheel_metadata_bytes = await get_wheel_metadata(sess, whl['href'], server_cfg)
                #     raw, unparsed = parse_email(wheel_metadata_bytes)
                #     if 'requires_dist' in raw:
                #         for dep in raw['requires_dist']:
                #             print("transitive:", dep)
                #             transitive_dep = Requirement(dep)
                #             if transitive_dep.marker:
                #                 # TODO(): all() or any()? How are extras evaluated????
                #                 # print("Marker Match:", any(transitive_dep.marker.evaluate(base_environment | {"extra": extra}) for extra in pkg_req.extras))
                #                 if any(transitive_dep.marker.evaluate({"extra": extra}) for extra in pkg_req.extras):
                #                     queue.append(transitive_dep)
                #                 elif transitive_dep.marker.evaluate(base_environment):
                #                     queue.append(transitive_dep)
                #             else:
                #                 queue.append(transitive_dep)
                # else:
                #     # Needs Building from source...???
                #     # print("SDIST is available")
                #     package_versions_resolved[pkg_name] = (pkg_req, "SDIST")
                #     pass

            # encountered this package again (must've been another package's transitive dependency with stricter versions)
            else:
                logger.info(f"BFS: {pkg_name} found in cache")
                # for k in package_versions_resolved:
                #     print(str(k))
                #     a,b = package_versions_resolved[k]
                #     print("\t", str(a))
                #     print("\t", str(b))
                older_req, _, _ = package_versions_resolved[pkg_name]
                updated_req = deepcopy(older_req)
                updated_req.extras.update(pkg_req.extras)
                updated_req.marker = _merger_markers(older_req.marker, pkg_req.marker)
                updated_req.specifier = _merge_specifiers(older_req.specifier, pkg_req.specifier)

                # whl, sdst = _pick_most_suitable(updated_req, url_db[pkg_name], valid_tags_profile)
                whl, sdst = _pick_most_suitable_from_lists(updated_req, url_db[pkg_name], valid_tags_profile)
                whl_version, sdst_version = None, None
                if whl:
                    _, whl_version, _, _ = parse_wheel_filename(whl.get_text(strip=True))
                if sdst:
                    _, sdst_version = parse_sdist_filename(sdst.get_text(strip=True))
                if whl is None and sdst is None:
                    # TODO(): Find out the Github
                    package_versions_resolved[pkg_name] = (updated_req, f"GITHUB-{updated_req.specifier}",
                                                           await try_finding_github(sess, pkg_name))
                    pass
                else:
                    if whl is None:
                        # TODO(): Only SDIST is available...try to figure out dependencies...
                        package_versions_resolved[pkg_name] = (updated_req, f"SDIST-{sdst_version}", sdst)
                        pass
                    # not sure if such a scenario does exist, but just to be sure
                    # scenario: wheel is available but no sdist is available
                    # UPDATE: This scenario may exist in custom pypi indices
                    elif sdst is None:
                        package_versions_resolved[pkg_name] = (updated_req,
                                                               parse_wheel_filename(whl.get_text(strip=True))[1], whl)
                        # print("ADDED", pkg_name, "to dict")
                        logger.info(f"Adding {pkg_name} to cache")

                        # add transitive dependencies
                        wheel_metadata_bytes = await get_wheel_metadata(sess, whl['href'], server_cfg)
                        raw, unparsed = parse_email(wheel_metadata_bytes)
                        if 'requires_dist' in raw:
                            for dep in raw['requires_dist']:
                                # print("transitive:", dep)
                                logger.info(f"Identified transitive dependency: {dep}")
                                transitive_dep = Requirement(dep)
                                if transitive_dep.marker:
                                    # TODO(): all() or any()? How are extras evaluated????
                                    # print("Marker Match:", any(transitive_dep.marker.evaluate(base_environment | {"extra": extra}) for extra in pkg_req.extras))
                                    if any(transitive_dep.marker.evaluate({"extra": extra}) for extra in
                                           pkg_req.extras):
                                        queue.append(transitive_dep)
                                    elif transitive_dep.marker.evaluate(base_environment):
                                        queue.append(transitive_dep)
                                else:
                                    queue.append(transitive_dep)
                        pass
                    # tricky scenario. pip prefers latest version
                    # if sdist is newer than whl, pip will use this
                    # when will this scenario trigger? - custom index has older wheels,
                    # upstream pypi has newer wheels(unsupported platform) and newer sdist
                    else:
                        if sdst_version > whl_version:
                            # only if we have a later sdist, pip will try to compile that
                            package_versions_resolved[pkg_name] = (updated_req, f"SDIST-{sdst_version}", sdst)
                            pass
                        else:
                            # yayyy we have a wheel
                            package_versions_resolved[pkg_name] = (updated_req,
                                                                   parse_wheel_filename(whl.get_text(strip=True))[1],
                                                                   whl)
                            # print("UPDATED", pkg_name, "in dict")
                            logger.info(f"Updating {pkg_name} in cache")
                            # add transitive dependencies
                            wheel_metadata_bytes = await get_wheel_metadata(sess, whl['href'], server_cfg)
                            raw, unparsed = parse_email(wheel_metadata_bytes)
                            if 'requires_dist' in raw:
                                for dep in raw['requires_dist']:
                                    # print("transitive:", dep)
                                    logger.info(f"Identified transitive dependency: {dep}")
                                    transitive_dep = Requirement(dep)
                                    if transitive_dep.marker:
                                        # TODO(): all() or any()? How are extras evaluated????
                                        # print("Marker Match:", any(transitive_dep.marker.evaluate(base_environment | {"extra": extra}) for extra in pkg_req.extras))
                                        if any(transitive_dep.marker.evaluate({"extra": extra}) for extra in
                                               pkg_req.extras):
                                            queue.append(transitive_dep)
                                        elif transitive_dep.marker.evaluate(base_environment):
                                            queue.append(transitive_dep)
                                    else:
                                        queue.append(transitive_dep)

                # TODO(): MAKE SURE TO UPDATE REQUIREMENTS IN package_versions_resolved EVEN IF IT IS UNAVAILABLE AS WHEELS
                # if whl is None and sdst is None:
                #     # TODO(): FIND GITHUB URL ??
                #     package_versions_resolved[pkg_name] = (updated_req, "GITHUB")
                #     pass
                # elif whl is not None:
                #     # choose this wheel for now
                #     # might change based on future explorations
                #     package_versions_resolved[pkg_name] = (updated_req,
                #                                            parse_wheel_filename(whl.get_text(strip=True))[1])
                #
                #     # add transitive dependencies
                #     wheel_metadata_bytes = await get_wheel_metadata(sess, whl['href'], server_cfg)
                #     raw, unparsed = parse_email(wheel_metadata_bytes)
                #
                #     if 'requires_dist' in raw:
                #             transitive_dep = Requirement(dep)
                #             if transitive_dep.marker:
                #                 # TODO(): all() or any()? How are extras evaluated????
                #                 if any(transitive_dep.marker.evaluate(base_environment | {"extra": extra}) for extra in updated_req.extras):
                #                     queue.append(transitive_dep)
                #                 elif transitive_dep.marker.evaluate(base_environment):
                #                     queue.append(transitive_dep)
                #             else:
                #                 queue.append(transitive_dep)
                # else:
                #     # Needs Building from source...???
                #     package_versions_resolved[pkg_name] = (updated_req, "SDIST")
                #     pass

    print("\n\n")
    print("ALL DONE".center(80, "="))
    for k in sorted(package_versions_resolved):
        a, b, c = package_versions_resolved[k]
        # print(str(k).ljust(30), str(b).ljust(10), "Source: ", c['href'] if isinstance(c, BSTag) else str(c))
        print(str(k).ljust(50),
              str(b).ljust(30) if not (str(b).startswith("SDIST") or str(b).startswith("GIT")) else
              c['href'].split("#")[0].split("/")[-1] if str(
                  b).startswith("SDIST") else str(c))


class ProfileLoader:
    def __init__(self, profile: str):
        if os.path.exists(profile):
            with open(profile) as prof:
                self.supported_tags = list(map(lambda tag: list(parse_tag(tag))[0], prof.read().strip().split('\n')))
        else:
            raise FileNotFoundError(
                f'Could not find "{profile}" in default profiles. Make sure file exists in the directory')


class EnvironmentLoader:
    def __init__(self, env_file: str):
        if os.path.exists(env_file):
            with open(env_file) as ev:
                self.env = json.load(ev)
        else:
            raise FileNotFoundError(
                f'Could not find "{env_file}". Make sure file exists in the directory')


class RequirementsReader:
    def __init__(self, requirements_file: str):
        if not os.path.exists(requirements_file):
            raise FileNotFoundError(f'Could not find "{requirements_file}"')
        with open(requirements_file) as req:
            self.requirements = [Requirement(r.split("#")[0].strip()) for r in req.readlines()
                                 if not r.startswith("#") and not r.startswith("-") and r.strip()]


class PipfileReader:
    def __init__(self, lock_file: str):
        if not os.path.exists(lock_file):
            raise FileNotFoundError(f'Could not find "{lock_file}"')
        with open(lock_file) as lock:
            self.requirements = [Requirement(f"{pkg}=={info['version'].lstrip('=')}")
                                 for pkg, info in json.load(lock).get('default').items()]

class ConfigReader:
    def __init__(self, config_file: str):
        if os.path.exists(config_file):
            with open(config_file) as cfg:
                self.config = safe_load(cfg)["index-urls"]
        else:
            raise FileNotFoundError(
                f'Could not find "{config_file}". Make sure the config yaml exists')


def main():
    parser = ArgumentParser()

    parser.add_argument("--profile", "-p",
                        help="Platform tags supported for the wheels to choose [defaults to current python interpreter]")
    parser.add_argument("--requirements", "-r", help="Requirements file")
    parser.add_argument("--pipfile", "-l", help="Pipfile.lock to read requirements")
    parser.add_argument("--config", "-c", help="Index Config YAML")
    parser.add_argument("--env", "-e", help="Environment json")

    args = parser.parse_args()

    if not args.profile:
        supported_tags = list(sys_tags())
    else:
        supported_tags = ProfileLoader(args.profile).supported_tags

    if not args.env:
        env = default_environment()
    else:
        env = EnvironmentLoader(args.env).env

    if args.requirements and args.pipfile:
        raise Exception("Can have either requirements.txt OR pipfile.lock")

    if args.requirements:
        requirements = RequirementsReader(args.requirements).requirements
    elif args.pipfile:
        requirements = PipfileReader(args.pipfile).requirements
    else:
        raise Exception("Need initial requirements or piplock")

    if not args.config:
        server_config = [{'url': PYPI_SIMPLE}]
    else:
        server_config = ConfigReader(args.config).config
    # requirements = [Requirement("pandas")]
    # requirements = [Requirement("beautifulsoup4")]
    requirements = list(filter(lambda req: not req.marker or (req.marker and req.marker.evaluate(env)), requirements))
    run(find_missing(requirements, supported_tags, env, {}, server_config))
