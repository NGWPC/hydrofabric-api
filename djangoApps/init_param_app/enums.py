from enum import StrEnum
from typing import List


####  These enums are used in validators
class FileTypeEnum(StrEnum):
    GEOPACKAGE = 'GEOPCKG'
    OBSERVATIONAL = 'OBSERVATIONAL'
    PARAMS = 'PARAMS'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]

class DomainEnum(StrEnum):
    ALASKA = 'Alaska'
    HAWAII = 'Hawaii'
    CONUS = 'CONUS'
    PUERTO_RICO = 'Puerto_Rico'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]


class StatusEnum(StrEnum):
    SAVED = 'Saved'
    READY = 'Ready'
    RUNNING = 'Running'
    DONE = 'Done'
    CANCELLED = 'Cancelled'
    FAILED = 'Failed'
    RESUMED = 'Resumed'
    SERVER_ERROR = 'Server error'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]


class CalibrationRunType(StrEnum):
    CALIB = 'calib'
    VALID_CONTROL = 'valid_control'
    VALID_BEST = 'valid_best'


class OptimizationEnum(StrEnum):
    DDS = 'DDS'
    GWO = 'GWO'
    PSO = 'PSO'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]


####  These enums are used in validators
class DataTypeEnum(StrEnum):
    DOUBLE = 'double'
    INTEGER = 'integer'
    BOOLEAN = 'boolean'
    STRING = 'string'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]


class LocationEnum(StrEnum):
    NODE = 'node'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]


class UnitsEnum(StrEnum):
    M = 'm'
    NONE = 'none'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]


class ForcingSourceEnum(StrEnum):
    AORC = 'AORC'
    UPLOAD = 'Upload'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]


class ObservationalSourceEnum(StrEnum):
    USGS = 'USGS'
    USACE = 'USACE'
    BOR = 'BOR'
    ENV = 'ENV'
    CA_DWR = 'CA DWR'
    TX_DOT = 'TX DoT'
    RFC = 'RFC'
    SNOTEL = 'SNOTEL'
    AGENCY = 'Agency'
    UPLOAD = 'Upload'

    @classmethod
    def values(cls) -> List[str]:
        # noinspection PyUnresolvedReferences
        return [e.value for e in cls]
