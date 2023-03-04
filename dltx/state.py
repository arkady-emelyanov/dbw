import os
import json

from typing import List, Dict, Any, AnyStr


# FIXME: different backends for state
class State:

    def __init__(self, workflow_name, params):
        self.workflow_name = workflow_name
        self.params = params
        self.internal_state = {}
        self.internal_state_path = self.params.get("state_file_storage_path")
        if not self.internal_state_path:
            raise Exception("'state_file_storage_path' is empty")

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
                self.internal_state = json.loads(f.read())
        self._unlock_state()

    def _format_file_path(self):
        return f"{self.internal_state_path}/{self.workflow_name}.json"

    def _save_state(self):
        self._lock_state()
        if not os.path.exists(self.internal_state_path):
            os.makedirs(self.internal_state_path, exist_ok=True)
        with open(self._format_file_path(), "w+") as f:
            json.dump(self.internal_state, f)
        self._unlock_state()

    # TODO: replace with object
    def update_state(self, events_list: List[Dict[AnyStr, Any]]):
        self._load_state()
        for event in events_list:
            obj = event["obj"]
            obj_id = event["obj_id"]
            if obj not in self.internal_state:
                self.internal_state[obj] = {}

            if event["action"] == "delete":
                if obj_id not in self.internal_state[obj]:
                    raise Exception(f"Unknown object ID to delete '{obj}' with {obj_id}")
                del self.internal_state[obj][obj_id]

            if event["action"] == "create":
                self.internal_state[obj][obj_id] = True

        self._save_state()

    def reset_state(self):
        self.internal_state = {}
        self._save_state()

    def get_state_map(self):
        self._load_state()
        if not len(self.internal_state):
            print("Empty state...")
        return self.internal_state.copy()
