"""HyperBackup task data (async updated)."""
from typing import Any, Dict, Optional
import json
from datetime import datetime
import logging

from synology_dsm.exceptions import SynologyDSMAPIErrorException

from .const import *

LOGGER = logging.getLogger(__name__)


class SynoBackup:
    """An implementation of Synology HyperBackup (now async)."""

    API_KEY = "SYNO.Backup.Task"
    API_KEY_TARGET = "SYNO.Backup.Target"
    STATUS_FIELDS = [PROP_LAST_BACKUP_TIME, PROP_NEXT_BACKUP_TIME, PROP_LAST_RESULT, PROP_LAST_PROGRESS]
    TARGET_FIELDS = [PROP_ONLINE, PROP_USED_SIZE]
    SYN_DATE_FORMAT = '%Y/%m/%d %H:%M'

    def __init__(self, dsm):
        """Initialize HyperBackup."""
        self._dsm = dsm
        self._data: Dict[int, Dict[str, Any]] = {}
        self._last_backup_times: Dict[int, str] = {}

    async def update(self, get_all_target_data=False):
        """Async update backup tasks settings and information from API."""
        prev_data = self._data
        self._data = {}

        LOGGER.debug("Executing %s API call for task list", self.API_KEY)
        task_list_resp = await self._dsm.get(self.API_KEY, "list", max_version=1)
        task_list = task_list_resp.get("data", {}).get("task_list", [])

        for task in task_list:
            task_id = task[PROP_TASKID]

            LOGGER.debug("Executing %s API call for task status details: %d", self.API_KEY, task_id)
            backup_status_resp = await self._dsm.get(
                self.API_KEY,
                "status",
                {PROP_TASKID: task_id, "additional": json.dumps(self.STATUS_FIELDS)},
                max_version=1
            )
            backup_status = backup_status_resp.get("data", {})
            task |= backup_status
            task.pop("schedule", None)
            task.pop("source", None)

            target_data = await self._get_target_data(task_id, task, prev_data, get_all_target_data)
            task |= target_data

            self._last_backup_times[task_id] = task[PROP_LAST_BACKUP_TIME]
            self._data[task_id] = task

    async def _get_target_data(self, task_id: int, task: Dict, prev_data: Dict, get_all_target_data=False) -> Dict:
        backup_since_last_update = (
            (task_id not in self._last_backup_times) or
            (task[PROP_LAST_BACKUP_TIME] != self._last_backup_times[task_id]) or
            not task[PROP_LAST_BACKUP_TIME]
        )

        if backup_since_last_update or get_all_target_data:
            try:
                LOGGER.debug("Making %s API call for task %d", self.API_KEY_TARGET, task_id)
                target_data_resp = await self._dsm.get(
                    self.API_KEY_TARGET,
                    "get",
                    {PROP_TASKID: task_id, 'additional': json.dumps(self.TARGET_FIELDS)},
                    max_version=1
                )
                target_data = target_data_resp.get("data", {})
            except SynologyDSMAPIErrorException:
                LOGGER.debug("target call failed for task %d, assuming target is offline", task_id)
                target_data = {PROP_ONLINE: False}
        else:
            target_data = prev_data.get(task_id, {})

        return {k: target_data.get(k, None) for k in self.TARGET_FIELDS}

    # Remaining properties and methods (unchanged):

    @property
    def task_ids(self):
        return self._data.keys()

    @property
    def tasks(self) -> Dict[int, Dict[str, Any]]:
        return self._data

    def get_task(self, task_id: int) -> Dict[str, Any]:
        return self._data[task_id]

    def health(self, task_id: int) -> str:
        ok_statuses = [STATUS_OK, STATUS_RESUMING, STATUS_WAITING, STATUS_RUNNING]
        if self.has_schedule(task_id) and self.status(task_id) in ok_statuses:
            return HEALTH_GOOD
        elif self.status(task_id) in [STATUS_RESTORE_ONLY, STATUS_ERROR]:
            return HEALTH_CRIT
        return HEALTH_WARN

    def status(self, task_id: int) -> str:
        raw_status = self.raw_status(task_id)
        state = self.state(task_id)
        previous_result = self.raw_previous_result(task_id)

        if state != STATE_BACKUP:
            if state == STATE_RESTORE_ONLY:
                return STATUS_RESTORE_ONLY
            elif state == STATE_ERROR and raw_status == PROP_STATUS_DETECT_WAIT:
                return STATUS_DETECT
            elif state in [STATE_ERROR, STATE_BROKEN, STATE_UNAUTH, STATE_END_SERVICE]:
                return STATUS_ERROR
            return STATUS_UNKNOWN

        if raw_status == PROP_STATUS_NONE:
            if previous_result == RESULT_DONE:
                return STATUS_OK if self.has_schedule(task_id) else STATUS_NO_SCHEDULE
            elif previous_result == RESULT_NONE:
                return STATUS_NEVER_RUN
            elif previous_result == RESULT_SUSPEND:
                return STATUS_SUSPENDED
            return STATUS_ERROR
        elif raw_status in [PROP_STATUS_BACKUP, PROP_STATUS_DETECT, PROP_STATUS_VER_DEL, PROP_STATUS_PREP_VER_DEL]:
            if previous_result == RESULT_RESUME:
                return STATUS_RESUMING
            if not self.has_schedule(task_id):
                return STATUS_RUNNING_NO_SCHEDULE
            return STATUS_RUNNING
        elif raw_status == PROP_STATUS_WAITING:
            return STATUS_WAITING
        return STATUS_ERROR

    def name(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_NAME)

    def has_schedule(self, task_id: int) -> bool:
        return bool(self.next_backup_time(task_id))

    def is_backing_up(self, task_id: int) -> bool:
        return bool(self.backup_progress(task_id) is not None)

    def backup_progress(self, task_id: int) -> Optional[int]:
        try:
            return self._data.get(task_id).get(PROP_PROGRESS).get(PROP_PROGRESS)
        except AttributeError:
            return None

    def state(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_STATE)

    def raw_status(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_STATUS)

    def target_id(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_TARGET_ID)

    def task_id(self, task_id: int) -> int:
        return self._data.get(task_id).get(PROP_TASKID)

    def transfer_type(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_TRANSFER_TYPE)

    def previous_result(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_LAST_RESULT).capitalize()

    def raw_previous_result(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_LAST_RESULT)

    def previous_backup_time(self, task_id: int) -> datetime:
        return self.to_datetime(self._data.get(task_id).get(PROP_LAST_BACKUP_END_TIME))

    def next_backup_time(self, task_id: int) -> datetime:
        return self.to_datetime(self._data.get(task_id).get(PROP_NEXT_BACKUP_TIME))

    def previous_error(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_LAST_BACKUP_ERROR)

    def target_online(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_ONLINE)

    def used_size(self, task_id: int) -> str:
        return self._data.get(task_id).get(PROP_USED_SIZE)

    @classmethod
    def to_datetime(cls, syn_datetime):
        if not syn_datetime:
            return None
        return datetime.strptime(syn_datetime, cls.SYN_DATE_FORMAT)
