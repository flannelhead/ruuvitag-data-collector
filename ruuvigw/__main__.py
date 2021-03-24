import sys
from time import sleep

import yaml
from bleson import get_provider, Observer
from influxdb_client import InfluxDBClient, Point, WriteOptions
from rx.scheduler import ThreadPoolScheduler
from rx.subject import Subject
from rx import operators as ops

from ruuvigw.parser import RuuviTagData


def _ruuvi_data_to_influx(data: RuuviTagData) -> Point:
    point = Point("ruuvi_measurements").tag("mac", data.mac).time(data.time)

    for field_name, val in data._asdict().items():
        if field_name != "mac" and field_name != "time" and val is not None:
            point.field(field_name, val)

    return point


if __name__ == "__main__":
    with open(sys.argv[1], "r") as _config_file:
        _config = yaml.safe_load(_config_file)

    _influx_config = _config["influxdb"]
    _influx_client = InfluxDBClient(
        _influx_config["url"],
        _influx_config["token"],
        org=_influx_config["org"],
        enable_gzip=True,
        verify_ssl=True,
    )
    _write_client = _influx_client.write_api(
        write_options=WriteOptions(
            flush_interval=15_000, max_retries=1, max_retry_delay=10_000
        )
    )

    _mac_list = [bytes.fromhex(x.replace(":", "")) for x in _config["macs"]]

    _measurements = Subject()
    thread_pool_scheduler = ThreadPoolScheduler(2)
    _measurements_subj = _measurements.pipe(ops.observe_on(thread_pool_scheduler))
    # _measurements_subj.subscribe(on_next=lambda x: print(x, flush=True))
    _write_client.write(
        bucket="sensors",
        record=_measurements_subj.pipe(ops.map(_ruuvi_data_to_influx)),
    )

    def on_advertisement(advertisement):
        msg = RuuviTagData.from_mfg_data(advertisement.mfg_data, _mac_list)
        if msg is not None:
            _measurements.on_next(msg)

    observer = Observer(get_provider().get_adapter())
    observer.on_advertising_data = on_advertisement
    observer.start()

    while True:
        sleep(10)
