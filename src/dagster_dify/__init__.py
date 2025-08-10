# noinspection PyProtectedMember
from dagster._core.libraries import DagsterLibraryRegistry
from .version import __version__

from .resources import DifyResource

DagsterLibraryRegistry.register("dagster-dingtalk", __version__)
