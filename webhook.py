import cvmfs
import re

def job(payload):
    if "events" in payload:
        # these events are from GitLab
        rootdir = ''
        for event in payload['events']:
            if is_tag_event(event):
                image_info = get_image_info(event)
                if is_accepted_tag(image_info.tag):
                    cvmfs.publish_docker_image(image_info,
                            'ligo-containers.opensciencegrid.org', rootdir)
                    return True
    elif "repository" in payload:
        # these events are from DockerHub
        rootdir = 'dockerhub'
        namespace = payload['repository']['namespace']
        project = payload['repository']['name']
        digest = None
        tag = payload['push_data']['tag']
        image_info = cvmfs.ImageInfo('', namespace, project, digest, tag)
        cvmfs.publish_docker_image(image_info,
            'ligo-containers.opensciencegrid.org', rootdir)
        return True
    else:
        return None

def is_tag_event(event):
    try:
        target = event['target']
        return (event['action'] == "push" and "tag" in target and
            target['mediaType'] == "application/vnd.docker.distribution.manifest.v2+json")
    except:
        return False

def is_accepted_tag(tag):
    explicit_tags = [ 'latest', 'nightly', 'master', 'production']

    # (1) matches arbirtary alphanumeric characters separated by periods
    # (2) matches ISO dates (no time) with optional alpha appended
    regex_tags = [ '^(\w+\.)*\w+$', '^\d{4}\-\d\d\-\d\d[a-zA-Z]?$' ]

    if tag in explicit_tags:
        return True

    for regex_tag in regex_tags:
        p = re.compile(regex_tag)
        if p.match(tag):
            return True

    return False

def get_image_info(event):
    try:
        return cvmfs.ImageInfo(event['request']['host'],
                   event['target']['repository'].rpartition("/")[0],
                   event['target']['repository'].rpartition("/")[2],
                   event['target']['digest'],
                   event['target']['tag'])
    except:
        return None
