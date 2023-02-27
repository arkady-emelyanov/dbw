from dltx.project import Project


class Variable:
    # looks like hack tbh
    _project_instance: Project = None

    @classmethod
    def get(cls, key):
        if not cls._project_instance:
            cls._project_instance = Project().configure({})

        return cls._project_instance \
            .params \
            .get(key)
