# NOTE:
# This file is forked from singularity 2.2's "cli.py".  I have left the
# copyright notice below untouched; this file remains under the same license.
# - Brian Bockelman

'''
bootstrap.py: python helper for Singularity command line tool
Copyright (c) 2016, Vanessa Sochat. All rights reserved.
"Singularity" Copyright (c) 2016, The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt of any
required approvals from the U.S. Dept. of Energy).  All rights reserved.

This software is licensed under a customized 3-clause BSD license.  Please
consult LICENSE file distributed with the sources of this project regarding
your rights to use or distribute this software.

NOTICE.  This Software was developed under funding from the U.S. Department of
Energy and the U.S. Government consequently retains certain rights. As such,
the U.S. Government has been granted for itself and others acting on its
behalf a paid-up, nonexclusive, irrevocable, worldwide license in the Software
to reproduce, distribute copies to the public, prepare derivative works, and
perform publicly and display publicly, and to permit other to do so.
'''

import sys
import docker
import os
import pathlib
import stat
import errno
import glob
import hashlib
import tempfile
import tarfile
import json
import shutil

class ImageInfo:
    def __init__(self, registry, namespace, project, digest, tag="latest"):
        self.registry = registry
        self.namespace = namespace
        self.project = project
        self.digest = digest
        self.tag = tag

    def name(self):
        return '/'.join(filter(None,[self.registry,self.namespace,self.project])) + ":" + self.tag

_in_txn = False

def abort_txn(filesystem):
    sys.stderr.write("Aborting transaction on %s!\n" % filesystem)
    return os.system("cvmfs_server abort -f %s" % filesystem)

def start_txn(filesystem):
    global _in_txn
    if _in_txn:
        return 0
    if os.path.exists("/var/spool/cvmfs/%s/in_transaction.lock" % filesystem):
        result = abort_txn(filesystem)
        if result:
            sys.stderr.write("Failed to abort lingering transaction (exit status %d).")
            return 1
    result = os.system("cvmfs_server transaction %s" % filesystem)
    if result:
        sys.stderr.write("Transaction start failed (exit status %d); will not attempt update." % result)
        return 1
    _in_txn = True

def publish_txn(filesystem):
    global _in_txn
    if _in_txn:
        _in_txn = False
        return os.system("cvmfs_server publish %s" % filesystem)
    return 0

# both paths absolute
def remove_publication(image_path, tag_link, publication_list_filename):

    # read in publication list
    with open(publication_list_filename, "r") as publication_list_fp:
        publication_list = json.load(publication_list_fp)

    # make modification in-memory
    publication_list['publications'][image_path].remove(tag_link)

    # re-publish entire list using "w" mode
    with open(publication_list_filename, "w") as publication_list_fp:
        json.dump(publication_list, publication_list_fp, indent=2,
            sort_keys=True)

    return 0

# both paths absolute
def add_publication(image_path, tag_link, publication_list_filename):

    # open publication list JSON or create empty JSON file in its place
    try:
        with open(publication_list_filename, "r") as publication_list_fp:
            publication_list = json.load(publication_list_fp)
    except FileNotFoundError:
        with open(publication_list_filename, "a+") as publication_list_fp:
            publication_list = {} 
            json.dump(publication_list, publication_list_fp)

    # add publication to list, creating necessary dict structure if non-existent
    try:
        publication_list['publications'][image_path].append(tag_link)
    except KeyError:
        if 'publications' not in publication_list:
            publication_list['publications'] = {}
        publication_list['publications'][image_path] = []
        publication_list['publications'][image_path].append(tag_link)

    # re-publish entire list using "w" mode
    with open(publication_list_filename, "w") as publication_list_fp:
        json.dump(publication_list, publication_list_fp, indent=2,
            sort_keys=True)

    return 0

def prune_publications(publication_list_filename):
    # read in publication list
    with open(publication_list_filename, "r") as publication_list_fp:
        publication_list = json.load(publication_list_fp)

    orphaned_publications = {k:v for (k,v) in publication_list['publications'].items() if not v}
    for orphan in orphaned_publications:
        orphan_path = pathlib.Path(orphan)
        shutil.rmtree(orphan)
        del publication_list['publications'][orphan]
        try:
            orphan_path.parent.rmdir()
        except OSError:
            continue

    # re-publish entire list using "w" mode
    with open(publication_list_filename, "w") as publication_list_fp:
        json.dump(publication_list, publication_list_fp, indent=2,
            sort_keys=True)


def create_symlink(image_relative_path, tag_link, filesystem_basepath, publication_list_filename):
    # check to ensure that we are in a transaction

    parent_dir = os.path.split(tag_link)[0]

    if not os.path.exists(parent_dir):
        try:
            os.makedirs(parent_dir)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise

    if not os.path.exists(tag_link):
        os.symlink(image_relative_path, tag_link)
        add_publication(os.path.join(filesystem_basepath, image_relative_path),
            tag_link, publication_list_filename)
    elif os.path.islink(tag_link):
        old_image_relative_path = os.readlink(tag_link)
        if old_image_relative_path != image_relative_path:
            os.unlink(tag_link)
            remove_publication(os.path.join(filesystem_basepath, old_image_relative_path),
                tag_link, publication_list_filename)
            os.symlink(image_relative_path, tag_link)
            add_publication(os.path.join(filesystem_basepath, image_relative_path),
                tag_link, publication_list_filename)
    else:
        return 1

    return 0

def write_docker_image(image_dir, image):
    # we should check to make sure that we're in a txn

    # will use a mix of Python 3.4 pathlib and old-style os module for now
    image_path = pathlib.Path(image_dir)

    status = os.system("docker run -v %s:/output --rm ligo/singularity:latest %s %s %s" % (image_dir, image, os.getuid(), os.getgid()) )
    if os.WEXITSTATUS(status) != 0:
        return False

    # Walk the path, fixing file permissions
    for (dirpath, dirnames, filenames) in os.walk(image_dir):
        for fname in filenames:
            full_fname = os.path.join(dirpath, fname)
            st = os.lstat(full_fname)
            old_mode = stat.S_IMODE(st.st_mode)
            if (old_mode & 0o0444) == 0o0000:
                new_mode = old_mode | 0o0400
                print("Fixing mode of", full_fname, "to", oct(new_mode))
                os.chmod(full_fname, new_mode)
        for dname in dirnames:
            full_dname = os.path.join(dirpath, dname)
            st = os.lstat(full_dname)
            if not stat.S_ISDIR(st.st_mode):
                continue
            old_mode = stat.S_IMODE(st.st_mode)
            if old_mode & 0o0111 == 0:
                new_mode = old_mode | 0o0100
                print(("Fixing mode of", full_dname, "to", oct(new_mode)))
                os.chmod(full_dname, new_mode)
            if old_mode & 0o0222 == 0:
                new_mode = old_mode | 0o0200
                print(("Fixing mode of", full_dname, "to", oct(new_mode)))
                os.chmod(full_dname, new_mode)

    # turns out to be a lot easier to add bind points and
    # de-publish directories if we have write perms on root path
    os.chmod(image_dir, 0o0755)

    # if the image contains a linux operating system, then add bind points
    if list(image_path.glob('etc/*-release')):
        bindpoints = [ 'srv', 'cvmfs', 'dev', 'proc', 'sys' ]
        for bindpoint in bindpoints:
            path_to_create = image_path / bindpoint
            path_to_create.mkdir(parents=False,exist_ok=True)

    # create .cvmfscatalog file so publishing indexes each container separately
    cvmfscatalog = image_path / '.cvmfscatalog'
    cvmfscatalog.touch()

    return True

def publish_docker_image(image_info, filesystem, rootdir='',
                  username=None, token=None):

    if image_info.digest is not None:
        digest = image_info.digest
    else:
        client = docker.from_env(timeout=3600)
        image = client.images.pull(image_info.name())
        digest = image.attrs['RepoDigests'][0].split('@')[1]
        client.images.remove(image_info.name())

    hash_alg, hash = digest.split(':')

    # start transaction to trigger fs mount
    retval = start_txn(filesystem)
    if retval:
        return retval

    # a single image can have multiple tags. User-facing directories with tags
    # should be symlinks to a single copy of the image
    publication_list_filename = os.path.join("/cvmfs", filesystem, rootdir,
        ".publications.json")
    filesystem_basepath = os.path.join("/cvmfs", filesystem, rootdir,
        image_info.namespace, image_info.project)
    digest_relative_path = os.path.join(".digests", hash_alg, hash[0:2], hash)
    image_dir = os.path.join(filesystem_basepath, digest_relative_path)
    tag_link = os.path.join(filesystem_basepath, image_info.tag)

    # if the path does not exist, use singularity to convert docker image to a
    # sandbox. If it does exist, simply create a new symlink
    if not os.path.exists(image_dir):
        os.makedirs(image_dir)
        if write_docker_image(image_dir, image_info.name()):
            create_symlink(digest_relative_path, tag_link, filesystem_basepath,
                publication_list_filename)
            prune_publications(publication_list_filename)
            publish_txn(filesystem)
        else:
            abort_txn(filesystem)
    else:
        create_symlink(digest_relative_path, tag_link, filesystem_basepath,
            publication_list_filename)
        prune_publications(publication_list_filename)
        publish_txn(filesystem)
