from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TimeWindow
import math
from typing import Iterable, Tuple
from utils.welford import update, finalize
from pyflink.common import Row


class TemperatureAggregate(AggregateFunction):
    def create_accumulator(self):
        return 0, 0.0, 0.0  # count, mean, M2

    def add(self, value: Row, accumulator: Tuple[int, float, float]) -> Tuple[int, float, float]:
        return update(accumulator, value.s194_temperature_celsius)

    def get_result(self, accumulator: Tuple[int, float, float]) -> Tuple[int, float, float]:
        return accumulator

    def merge(self, a: Tuple[int, float, float], b: Tuple[int, float, float]) -> Tuple[int, float, float]:
        count_a, mean_a, M2_a = a
        count_b, mean_b, M2_b = b

        count = count_a + count_b
        delta = mean_b - mean_a
        mean = mean_a + delta * count_b / count
        M2 = M2_a + M2_b + delta * delta * count_a * count_b / count

        return count, mean, M2


class ComputeStats(ProcessWindowFunction):
    def process(
            self,
            key: int,
            context: ProcessWindowFunction.Context,
            stats: Iterable[Tuple[int, float, float]],
    ) -> Iterable[Row]:
        count, mean, M2 = next(iter(stats))
        mean, variance, sample_variance = finalize((count, mean, M2))
        stddev = math.sqrt(variance) if count > 1 else 0.0

        window: TimeWindow = context.window()

        result = Row(ts=window.start, vault_id=key, count=count, mean_s194=mean, stddev_s194=stddev)

        # Log the results before writing to Kafka
        yield result
