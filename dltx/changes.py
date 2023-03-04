class Changes:

    def __init__(self):
        self.changes = []
        self.state = {}

    def create(self, obj: str, obj_id: str):
        self.changes.append({
            "action": "create",
            "obj": obj,
            "obj_id": obj_id,
        })

    def delete(self, obj: str, obj_id: str):
        self.changes.append({
            "action": "delete",
            "obj": obj,
            "obj_id": obj_id,
        })

    def finalize(self):
        return self.changes
