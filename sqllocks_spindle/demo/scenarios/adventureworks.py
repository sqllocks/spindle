"""adventureworks scenario registration."""
from sqllocks_spindle.demo.catalog import get_catalog, ScenarioMeta


def register():
    cat = get_catalog()
    # Already registered via ScenarioCatalog builtins — this is a no-op for built-ins.
    pass
