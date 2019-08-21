import jsonpickle
import caom2
import datetime
from caom2pipe import manage_composable as mc


class TM(jsonpickle.handlers.BaseHandler):

    def restore(self, data):
        result = None
        import logging
        logging.error('being called here?')
        logging.error('and here? type {}'.format(type(data)))
        unpickler = self.context
        restore = unpickler.restore
        if data['__dict__'] is not None:
            if data['__dict__']['_oktypes'] is not None:
                class_name = data['__dict__']['_oktypes']['py/type']
                if class_name is not None:
                    parts = class_name.split('.')
                    logging.error('parts are {}'.format(parts))
                    if len(parts) != 3:
                        raise Exception('stop now')
                    import importlib
                    m = importlib.import_module(parts[0], parts[1])
                    # TODO TODO TODO - why do I need the strip here?
                    klass = getattr(m, parts[2].strip())
                    result = caom2.caom_util.TypedOrderedDict(klass)
                    logging.error('class name of TypedOrderedDict {}'.format(class_name))
                    for k, v in data.items():
                        # logging.error('k {} v {}'.format(k, v))
                        if k != '__dict__' and k != 'py/object':
                            value = restore(v, reset=False)
                            value.key = k
                            logging.error('k {} type is {}'.format(k, type(value)))
                            result.add(value)
        return result


class DateTimeHandler(jsonpickle.handlers.BaseHandler):

    def flatten(self, obj, data):
        return obj.isoformat()

    def restore(self, data):
        cls, args = data['__reduce__']
        unpickler = self.context
        restore = unpickler.restore
        cls = restore(cls, reset=False)
        value = datetime.datetime.strptime(args[0], '%Y-%m-%dT%H:%M:%S.%f')
        return value


jsonpickle.handlers.register(caom2.caom_util.TypedOrderedDict, TM)
jsonpickle.handlers.register(datetime.datetime, DateTimeHandler)

# f = open('/app/draost2caom2/draost2caom2/tests/data/qz.json.orig')
# f = open('/app/draost2caom2/draost2caom2/tests/data/JW22.output.json')
# f = open('/app/draost2caom2/draost2caom2/tests/data/output.json')
# f = open('/app/draost2caom2/draost2caom2/tests/data/output.rn43.json')
f = open('/usr/src/app/draost2caom2/draost2caom2/tests/data/output.jul.json')
# f = open('/app/draost2caom2/draost2caom2/tests/data/sample_obs.json')
js = f.read()
x = jsonpickle.decode(js)
f.close()
# print(x.planes['JW22-C74'].polarization)

x._id = caom2.AbstractCaomEntity._gen_id(fulluuid=False)
x._last_modified = datetime.datetime.now()
x._max_last_modified = datetime.datetime.now()
x._meta_checksum = None

if x._proposal is not None:
    if not hasattr(x._proposal, '_project'):
        x._proposal._project = None
    if not hasattr(x._proposal, '_name'):
        x._proposal._name = None
    if not hasattr(x._proposal, '_keywords'):
        x._proposal._keywords = set()
    if not hasattr(x._proposal, '_title'):
        x._proposal._title = None
if x._target is not None:
    x._target._keywords = set()
x._requirements = None
if x._telescope is not None:
    x._telescope._keywords = set()
if x._instrument is not None:
    x._instrument._keywords = set()
x._environment = None


for plane in x.planes.values():
    plane._id = caom2.AbstractCaomEntity._gen_id(fulluuid=False)
    plane._last_modified = datetime.datetime.now()
    plane._max_last_modified = datetime.datetime.now()
    plane._meta_checksum = None
    plane._acc_meta_checksum = None
    plane._metrics = None
    plane._quality = None
    # plane._calibration_level = None
    if plane._provenance is not None:
        plane._provenance._keywords = set()
        plane._provenance._inputs = caom2.caom_util.TypedSet(caom2.PlaneURI, )
        # plane._provenance._last_executed = None
    if hasattr(plane, '_position'):
        if plane._position is not None:
            plane._position._dimension = None
            plane._position._resolution = None
            plane._position._sample_size = None
    else:
        plane._position = None
    if hasattr(plane, '_energy'):
        if plane._energy is not None:
            plane._energy._sample_size = None
            plane._energy._bandpass_name = None
            plane._energy._transition = None
    else:
        plane._energy = None
    if not hasattr(plane, '_polarization'):
        plane._polarization = None
    if not hasattr(plane, '_time'):
        plane._time = None
    if not hasattr(plane, '_position'):
        plane._position = None

    # plane.artifacts = caom2.caom_util.TypedOrderedDict(caom2.Artifact, )
    for artifact in plane.artifacts.values():
        artifact._id = caom2.AbstractCaomEntity._gen_id(fulluuid=False)
        artifact._last_modified = datetime.datetime.now()
        artifact._max_last_modified = datetime.datetime.now()
        artifact._meta_checksum = None
        artifact._acc_meta_checksum = None
        artifact.parts = caom2.caom_util.TypedOrderedDict(caom2.Part,)


# u = jsonpickle.unpickler.Unpickler()
# u.register_classes(caom2.caom_util.TypeOrderedDict)
# y = u.restore(js)
# print(y)

# without the register_classes, this gives the js as a very
# nicely-formatted string ;)
# y = u.restore(js)
# print(y)

mc.write_obs_to_file(x, './draost.xml')
print(x.planes['RN43-C74'].polarization)
