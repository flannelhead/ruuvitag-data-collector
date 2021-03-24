from datetime import datetime, timezone
from math import ceil, log10
from struct import calcsize, unpack_from
from typing import NamedTuple, List, Optional


_RUUVITAG_DATA_PREFIX = b"\x99\x04\x05"
_RUUVITAG_DATA_FORMAT = ">hHHhhhHBH"
_RUUVITAG_PREFIX_SIZE = len(_RUUVITAG_DATA_PREFIX)
_RUUVITAG_DATA_SIZE = calcsize(_RUUVITAG_DATA_FORMAT)
_RUUVITAG_MAC_SIZE = 6
_RUUVITAG_MSG_SIZE = _RUUVITAG_PREFIX_SIZE + _RUUVITAG_DATA_SIZE + _RUUVITAG_MAC_SIZE
_RUUVITAG_DATA_OFFSET = _RUUVITAG_PREFIX_SIZE
_RUUVITAG_MAC_OFFSET = _RUUVITAG_DATA_OFFSET + _RUUVITAG_DATA_SIZE
_INT16_MIN = -(2 ** 15)
_UINT16_MAX = 2 ** 16 - 1


class RuuviTagRawData(NamedTuple):
    temperature: int
    humidity: int
    pressure: int
    accel_x: int
    accel_y: int
    accel_z: int
    power_info: int
    movement_count: int
    measurement_sequence_number: int


class RuuviTagData(NamedTuple):
    mac: Optional[str]
    time: Optional[datetime]
    temperature: Optional[float]
    humidity: Optional[float]
    pressure: Optional[float]
    accelerationX: Optional[int]
    accelerationY: Optional[int]
    accelerationZ: Optional[int]
    batteryVoltage: Optional[float]
    txPower: Optional[int]
    movementCounter: Optional[int]
    measurementSequenceNumber: Optional[int]

    @staticmethod
    def from_mfg_data(
        data: Optional[bytes], macs: List[bytes]
    ) -> Optional["RuuviTagData"]:
        if not (
            data
            and len(data) == _RUUVITAG_MSG_SIZE
            and data.startswith(_RUUVITAG_DATA_PREFIX)
            and any(data.startswith(mac, _RUUVITAG_MAC_OFFSET) for mac in macs)
        ):
            return None

        mac = ":".join(
            f"{c:02X}" for c in data[_RUUVITAG_MAC_OFFSET:_RUUVITAG_MSG_SIZE]
        )

        raw = RuuviTagRawData._make(
            unpack_from(_RUUVITAG_DATA_FORMAT, data, _RUUVITAG_PREFIX_SIZE)
        )
        return RuuviTagData(
            mac=mac,
            time=datetime.now(timezone.utc),
            temperature=_convert(raw.temperature, 0.005),
            humidity=_convert(raw.humidity, 0.0025, invalid=_UINT16_MAX),
            pressure=_convert(raw.pressure, 0.01, 500.0, invalid=_UINT16_MAX),
            accelerationX=_convert_int(raw.accel_x),
            accelerationY=_convert_int(raw.accel_y),
            accelerationZ=_convert_int(raw.accel_z),
            batteryVoltage=_convert(
                raw.power_info >> 5, scale=0.001, constant=1.6, invalid=0x7FF
            ),
            txPower=_convert_int(raw.power_info & 0x1F, 2.0, -40.0, invalid=0x1F),
            movementCounter=_convert_int(raw.movement_count, invalid=0xFF),
            measurementSequenceNumber=_convert_int(
                raw.measurement_sequence_number, invalid=_UINT16_MAX
            ),
        )


def _convert_inner(
    value_in: int, scale: float, constant: float, invalid: int
) -> Optional[float]:
    if value_in == invalid:
        return None
    return value_in * scale + constant


def _convert(
    value_in: int, scale: float = 1.0, constant: float = 0.0, invalid: int = _INT16_MIN
) -> Optional[float]:
    out = _convert_inner(value_in, scale, constant, invalid)
    if out is None:
        return None
    ndigits = ceil(-log10(scale))
    return round(out, ndigits)


def _convert_int(
    value_in: int, scale: float = 1.0, constant: float = 0.0, invalid: int = _INT16_MIN
) -> Optional[int]:
    out = _convert_inner(value_in, scale, constant, invalid)
    if out is None:
        return None
    return int(round(out))
