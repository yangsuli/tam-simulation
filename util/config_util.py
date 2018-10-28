import ast


class SimConfigExpection(Exception):
    pass


class SimConfBase(object):
    def __init__(self):
        self.configs = {}
        self.str_buf = "SimConfBase:"

    # transform each config item's data type from string to target (eg. int, double, list, boolean ...)
    def polish_attr_type(self):
        for key, val in self.configs.iteritems():
            print val
            self.configs[key] = ast.literal_eval(val)

    def load_config(self, config, sec):
        assert sec in config.sections()
        opts = config.options(sec)
        for opt in opts:
            self.configs[opt] = config.get(sec, opt)
        self.polish_attr_type()

    def __str__(self):
        return self.str_buf + str(self.configs)
