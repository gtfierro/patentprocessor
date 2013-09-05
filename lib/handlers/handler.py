class Patobj(object):
    pass

class PatentHandler(object):
    def get_patobj(self):
        patobj = Patobj()
        for attr in self.attributes:
            patobj.__dict__[attr] = getattr(self, attr)
        return patobj