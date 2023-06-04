import re
from typing import Optional

from paramiko import SFTP_NO_SUCH_FILE

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sensors.base import BaseSensorOperator


class SFTPSensorWildcard(BaseSensorOperator):
    """
    Waits for a file or directory to be present on SFTP.

    :param path: Remote directory path
    :type path: str
    :param file_wc: Remote file with wildcard
    :type file_wc: str
    :param sftp_conn_id: The connection to run the sensor against
    :type sftp_conn_id: str
    """

    template_fields = (
        "path",
        "file_wc",
    )

    def __init__(self, *, path: str, file_wc: str, sftp_conn_id: str = "sftp_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.file_wc = file_wc
        self.hook: Optional[SFTPHook] = None
        self.sftp_conn_id = sftp_conn_id

    def poke(self, context: dict) -> bool:
        self.hook = SFTPHook(self.sftp_conn_id)

        files = self.hook.list_directory(self.path)  # List all files in a path
        self.log.info(f"The files in this path {self.path} are {str(files)}")
        for file in files:
            # if condition in file:
            regex_val = re.search(self.file_wc, file)
            if regex_val != None:
                self.file_wc = file
                break

        path_file = self.path + self.file_wc
        self.log.info(f"Poking for {path_file}")

        try:
            mod_time = self.hook.get_mod_time(path_file)
            self.log.info("Found File %s last modified: %s", str(path_file), str(mod_time))
        except OSError as e:
            if e.errno != SFTP_NO_SUCH_FILE:
                raise e
            return False
        self.hook.close_conn()
        return True
