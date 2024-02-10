from configparser import ConfigParser


def get_config(config_file='config.ini'):
    """
    Retrieve configurations from the specified config file.

    Args:
    - config_file (str): Path to the configuration file. Default is 'config.ini'.

    Returns:
    - dict: A dictionary containing the configurations.
    """
    # Create a ConfigParser object
    config_parser = ConfigParser()

    # Read the configuration file
    config_parser.read(config_file)
    sections = config_parser.sections()
    configs = configs = {s: {o: config_parser.get(s, o) for o in config_parser.options(s)} for s in sections}
    return configs