import js

from importlib import resources
from pathlib import Path
from shutil import copytree


def setup_emscripten_timezone_database():
    """Setup the timezone database for use on browser based
    emscripten environments.
    """
    if not Path("/usr/share/zoneinfo").exists():
        Path("/usr/share/").mkdir(parents=True, exist_ok=True)
        try:
            tzpath = resources.files("tzdata").joinpath("zoneinfo")
            copytree(tzpath, "/usr/share/zoneinfo", dirs_exist_ok=True)
            localtime_path = Path("/etc/localtime")
            if not localtime_path.exists():
                # get local timezone from browser js object
                timezone = js.Intl.DateTimeFormat().resolvedOptions().timeZone
                if timezone and str(timezone) != "":
                    timezone = str(timezone)
                    # make symbolic link to local time
                    Path("/etc/").mkdir(parents=True, exist_ok=True)
                    localtime_path.symlink_to(tzpath / timezone)
        except (ImportError, IOError):
            print("Arrow couldn't install timezone db to /usr/share/zoneinfo")
