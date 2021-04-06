from archery.utils.source import ArrowSources
from archery.crossbow import Config


def test_config():
    src = ArrowSources.find()
    conf = Config.load_yaml(src.dev / "tasks" / "tasks.yml")
    conf.validate()
