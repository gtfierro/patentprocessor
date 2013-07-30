"""
These functions support schema so it doesn't get too bloated
"""


def fetch(clean, matchSet, session, default):
    """
    Takes the values in the existing parameter.
    If all the tests in matchset pass, it returns
    the object related to it.

    if the params in default refer to an instance that exists,
    return it!
    """
    for keys in matchSet:
        cleanCnt = session.query(clean)
        keep = True
        for k in keys:
            if k not in default:
                keep = False
                break
            cleanCnt.filter(clean.__dict__[k] == default[k])
        if keep and cleanCnt.count() > 0:
            return cleanCnt.first()
    return None
