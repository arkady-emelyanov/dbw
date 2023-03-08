import os
import json

from typing import Dict, AnyStr, Any


class Changes:

    def __init__(self, workflow_name: str, params: Dict[AnyStr, Any]):
        self.workflow_name = workflow_name
        self.objects = []
        self.internal_state_path = params.get("state_file_storage_path")
        if not self.internal_state_path:
            raise Exception("Empty 'state_file_storage_path'!")
        self._load_state()

    def add_object(self, obj_type: str, obj_id: str):
        self.objects.append({"obj_type": obj_type, "obj_id": obj_id})
        self._save_state()

    def get_objects(self):
        return self.objects.copy()

    def reset(self):
        self.objects = []
        self._save_state()

    def _lock_state(self):
        pass

    def _unlock_state(self):
        pass

    def _has_stored_state(self):
        if os.path.exists(self._format_file_path()):
            return True
        return False

    def _load_state(self):
        self._lock_state()
        if self._has_stored_state():
            with open(self._format_file_path(), "rb") as f:
                self.objects = json.load(f)
        self._unlock_state()

    def _format_file_path(self):
        return f"{self.internal_state_path}/{self.workflow_name}.json"

    def _save_state(self):
        self._lock_state()
        if not os.path.exists(self.internal_state_path):
            os.makedirs(self.internal_state_path, exist_ok=True)
        with open(self._format_file_path(), "w+") as f:
            json.dump(self.objects, f)
        self._unlock_state()
