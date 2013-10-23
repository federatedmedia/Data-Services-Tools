from ConfigParser import SafeConfigParser, NoOptionError 

class ConfigException(Exception):
    pass

class DSConfig(object):
    """
    @summary: DataServices config object
    Store the result of this class' instantiation and use to retrieve config values 
    from a preconfigured resource i.e. an .ini config file or ZooKeeper
    """
    _zookeeper_bin = '/usr/local/bin/zk'
    
    def __init__(self, app_name='DSApp', **kwargs):
        """
        <app name>.base.ini defines all known config directives/values for DS apps
        any values redefined in {environment}.ini will override the base.ini value
        
        @param app: section within the configuration files to load (should match app name)
        
        @param kwargs: { environment, use_zookeeper, zk_base_path, dict_type }
            environment: <string>
            use_zookeeper: <boolean>
            zk_base_path: <string>
            dict_type: <callable python dict object name> for SafeConfigParser init 
        
        Configuration file notes:    
            if not using ZK, path to filename containing config
            if using ZK, config values will be polled from the local zookeeper app (defined in self._zookeeper_bin) 
 
        ZooKeeper note: full ZK path = zk_base_path/zk_key
        """             
        self._app = {'name':app_name, 'environment': kwargs.get('environment', 'dev'), 
                     'use_zookeeper':kwargs.get('use_zookeeper', False) , 'zk_base_path':kwargs.get('zk_base_path', '/'), 
                     'dict_type':kwargs.get('dict_type')}
        
        if self._app['use_zookeeper'] in [False, None]:
            self._init_config_files()


    def _init_config_files(self):
        """Initializes the config parser and reads config .ini files from a few choice locations:
        ./ config/ <passed in arg, environment>"""
        from os import path, curdir
        import sys

        if self._app['dict_type'] is not None:
            self._config = SafeConfigParser(dict_type=self._app['dict_type'])
        else:
            self._config = SafeConfigParser()
        
        # try to find the importing file using sys.argv[0]
        calling_file = sys.argv[0]
        if calling_file == '':
            abs_path = path.abspath(path.dirname(__file__))
        else:
            abs_path = path.abspath(path.dirname(calling_file))
        if abs_path == '/':
            abs_path = curdir
            
        # load base.ini always
        # then see if the specified environment has a config override, if so, try and load it           
        potential_config_files = ["%s.base.ini" % self._app['name'],
                                  path.join(abs_path, "config", "%s.base.ini" % self._app['name']), 
                                  "%s.ini" % self._app['environment'],
                                  path.join(abs_path, "config", "%s.ini" % self._app['environment']),
                                  path.join(abs_path, "config", path.basename(self._app['environment'])),
                                  self._app['environment']
                                 ]
        configs_read = self._config.read(potential_config_files)
        if len(configs_read) == 0:
            raise ConfigException("No configuration files for '%s' app found. Looked for the following: %s" % (self._app['name'], str(potential_config_files)))


    def get(self, option, option_type=None):
        """
        Get config option value as defined in the config files
        if config directive is not found, throw ConfigException
        _get_config specific parameters:
        @param option: <string> directive defined in base config file
        @param type: int, float or boolean
        """
        try:
            if self._app['use_zookeeper'] is True:
                return self._getZookeeper(option)
            elif option_type is None:
                return self._config.get(self._app['name'], option)
            elif option_type == 'list':
                return self._getList(self._config.get(self._app['name'], option))
            else:
                funct = getattr(self._config, 'get' + option_type)
                return funct(self._app['name'], option)
        except NoOptionError, msg:
            raise ConfigException(msg)
    
    def items(self, section=None):
        if section is not None and self._config.has_section(section):
            return self._config.items(section)
        else:
            return False
    
    def has_section(self, section=None):
        return self._config.has_section(section)

    def has_option(self, option=None, section=None):
        if section is None:
            section = self._app['name']
        return self._config.has_option(section, option)

    def raw_parser_method(self, method):
        """
        Allow "raw" access to ConfigParser methods
        @param function name requested
        """
        try:
            func = getattr(self._config, method)
            return func
        except AttributeError:
            raise ConfigException("%s is not a valid object method" % method)
    
    def _getList(self, config_value):
        """
        Converts a string config value to a list
        @param config_value: csv string to split into a list
        """
        return [x.strip() for x in config_value.split(',')]


    def _getZookeeper(self, zk_key):
        """
        Fetches ZooKeeper values from the CLI
        TODO: replace these lookups with python-zookeeper native module calls.
        @param zk_key: sookeeper path to key to fetch
        """
        import subprocess
        zk_proc = subprocess.Popen([self._zookeeper_bin, "get", "%s/%s" % (self._app['zk_base_path'], zk_key)], 
                                   stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        zk_proc.wait()
        zk_value, err = zk_proc.communicate()
        if err:
            raise ConfigException("Error running zookeeper: %s" % err)
        else:
            if zk_value.count("FATAL") > 0:
                raise ConfigException("ZooKeeper error: %s" % zk_value)
            else:
                return zk_value.strip()
