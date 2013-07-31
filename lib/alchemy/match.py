from collections import defaultdict
from collections import Counter


def match(objects, session, default={}, keepexisting=False, commit=True):
    """
    Pass in several objects and make them equal

    Args:
        objects: A list of RawObjects like RawAssignee
          also supports CleanObjects like Assignee
        keepexisting: Keep the default keyword
        default: Fields to default the clean variable with

        Default key priority:
        auto > keepexisting > default
    """
    if type(objects).__name__ in ('list', 'tuple'):
        objects = list(set(objects))
    elif type(objects).__name__ not in ('list', 'tuple', 'Query'):
        objects = [objects]
    freq = defaultdict(Counter)
    param = {}
    raw_objects = []
    clean_objects = []
    clean_cnt = 0
    clean_main = None
    class_type = None

    for obj in objects:
        if obj.__tablename__[:3] == "raw":
            clean = obj.__clean__
            if not class_type:
                class_type = obj.__related__
        else:
            clean = obj
            obj = None
            if not class_type:
                class_type = clean.__class__

        if clean and clean not in clean_objects:
            clean_objects.append(clean)
            if len(clean.__raw__) > clean_cnt:
                clean_cnt = len(clean.__raw__)
                clean_main = clean
            # figures out the most frequent items
            if not keepexisting:
                for k in clean.__related__.summarize:
                    freq[k] += Counter(dict(clean.__rawgroup__(session, k)))
        elif obj and obj not in raw_objects:
            raw_objects.append(obj)

    # this function does create some slow down
    if class_type and default:
        fetched = class_type.fetch(session, default)
        if fetched:
            clean_main = fetched

    exist_param = {}
    if clean_main:
        exist_param = clean_main.summarize

    if keepexisting:
        param = exist_param
    else:
        param = exist_param
        for obj in raw_objects:
            for k, v in obj.summarize.iteritems():
                if k not in default:
                    freq[k][v] += 1
            if "id" not in exist_param:
                if "id" not in param:
                    param["id"] = obj.uuid
                param["id"] = min(param["id"], obj.uuid)

    # create parameters based on most frequent
    for k in freq:
        if None in freq[k]:
            freq[k].pop(None)
        if "" in freq[k]:
            freq[k].pop("")
        if freq[k]:
            param[k] = freq[k].most_common(1)[0][0]
    param.update(default)

    # remove all clean objects
    if len(clean_objects) > 1:
        for obj in clean_objects:
            clean_main.relink(session, obj)
        session.commit()  # commit necessary

        # for some reason you need to delete this after the initial commit
        for obj in clean_objects:
            if obj != clean_main:
                session.delete(obj)

    if clean_main:
        relobj = clean_main
        relobj.update(**param)
    else:
        relobj = class_type(**param)
    # associate the data into the related object

    for obj in raw_objects:
        relobj.relink(session, obj)

    session.merge(relobj)
    if commit:
        session.commit()


def unmatch(objects, session):
    """
    Separate our dataset
    # TODO. THIS NEEDS TO BE FIGURED OUT
    # Unlinking doesn't seem to be working
    # properly if a LOCATION is added
    """
    if type(objects).__name__ in ('list', 'tuple'):
        objects = list(set(objects))
    elif type(objects).__name__ not in ('list', 'tuple', 'Query'):
        objects = [objects]
    for obj in objects:
        if obj.__tablename__[:3] == "raw":
            obj.unlink(session)
        else:
            session.delete(obj)
            session.commit()
