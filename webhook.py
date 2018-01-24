import cvmfs

def job(payload):
    if "events" in payload:
        rootdir = ''
        for event in payload['events']:
            if is_tag_event(event):
                image_info = get_image_info(event)
                cvmfs.publish_docker_image(image_info,
                    'ligo-containers.opensciencegrid.org', rootdir)
                return True
    elif "repository" in payload:
        rootdir = 'dockerhub'
        namespace = payload['repository']['namespace']
        project = payload['repository']['name']
        tag = payload['push_data']['tag']
        image_info = cvmfs.ImageInfo('', namespace, project, tag)
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

def get_image_info(event):
    try:
        return cvmfs.ImageInfo(event['request']['host'],
                   event['target']['repository'].rpartition("/")[0],
                   event['target']['repository'].rpartition("/")[2],
                   event['target']['tag'])
    except:
        return None
