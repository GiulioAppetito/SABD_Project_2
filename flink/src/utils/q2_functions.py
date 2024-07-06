import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from pyflink.common import WatermarkStrategy
from pyflink.datastream.window import TumblingEventTimeWindows, Time, SlidingEventTimeWindows
from pyflink.datastream.functions import AggregateFunction, ProcessAllWindowFunction, ReduceFunction
from typing import List, Tuple, Iterable, Dict
from utils.utils import MyTimestampAssigner
from pyflink.datastream.window import TimeWindow

# Configura il logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class VaultFailuresPerDayReduceFunction(ReduceFunction):
    def reduce(self, value1: Row, value2: Row) -> Row:
        date: str = value1['date']
        vault_id: int = value1['vault_id']
        failures_count: int = value1['failures_count'] + value2['failures_count']
        failed_disks: List[str] = value1['failed_disks'] + value2['failed_disks']
        return Row(vault_id=vault_id, date=date, failures_count=failures_count, failed_disks=failed_disks)


class VaultFailuresAggregateFunction(AggregateFunction):
    def create_accumulator(self) -> Dict[Tuple[int, str], Tuple[int, List[str]]]:
        return {}

    def add(self, value: Row, accumulator: Dict[Tuple[int, str], Tuple[int, List[str]]]) -> Dict[Tuple[int, str], Tuple[int, List[str]]]:
        vault_id: int = value['vault_id']
        date: str = value['date']
        failures_count: int = value['failures_count']
        failed_disks: List[str] = value['failed_disks']
        key: Tuple[int, str] = (vault_id, date)

        if key not in accumulator:
            accumulator[key] = (failures_count, failed_disks)
        else:
            current_failures, current_disks = accumulator[key]
            new_failures: int = current_failures + failures_count
            new_disks: List[str] = current_disks + failed_disks
            accumulator[key] = (new_failures, new_disks)

        return accumulator

    def get_result(self, accumulator: Dict[Tuple[int, str], Tuple[int, List[str]]]) -> List[Tuple[int, Tuple[int, List[str]]]]:
        max_failures_per_vault: Dict[int, Tuple[int, List[str]]] = {}
        for (vault_id, date), (failures_count, failed_disks) in accumulator.items():
            if vault_id not in max_failures_per_vault or failures_count > max_failures_per_vault[vault_id][0]:
                max_failures_per_vault[vault_id] = (failures_count, failed_disks)

        ranking: List[Tuple[int, Tuple[int, List[str]]]] = sorted(max_failures_per_vault.items(), key=lambda x: x[1][0], reverse=True)[:10]
        return ranking

    def merge(self, acc1: Dict[Tuple[int, str], Tuple[int, List[str]]], acc2: Dict[Tuple[int, str], Tuple[int, List[str]]]) -> Dict[Tuple[int, str], Tuple[int, List[str]]]:
        for key, value in acc2.items():
            if key not in acc1:
                acc1[key] = value
            else:
                current_failures, current_disks = acc1[key]
                new_failures: int = current_failures + value[0]
                new_disks: List[str] = current_disks + value[1]
                acc1[key] = (new_failures, new_disks)
        return acc1


class VaultFailuresProcessFunction(ProcessAllWindowFunction):
    def process(
            self,
            context: ProcessAllWindowFunction.Context,
            elements: Iterable[List[Tuple[int, Tuple[int, List[str]]]]],
    ) -> Iterable[Row]:
        ranking: List[Tuple[int, Tuple[int, List[str]]]] = next(iter(elements))
        window: TimeWindow = context.window()

        row_data: List = [window.start]
        for i in range(10):
            if i < len(ranking):
                vault_id, (failures_count, failed_disks) = ranking[i]
                row_data.extend([vault_id, failures_count, failed_disks])
            else:
                row_data.extend([None, None, None])

        yield Row(*row_data)
